// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"


#define ECHO_LISTEN_PORT		8888
#define MAX_EVENTS 10
#define BIGGER_BUFSIZE 2097153

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static io_context_t ctx;


static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/* Prepare the connection buffer to send the reply header. */
	const char *response_code;

	if (conn->state == STATE_SENDING_DATA || conn->state == STATE_SENDING_HEADER || conn->state == STATE_DATA_SENT)
		response_code = "200 OK";
	else if (conn->state == STATE_SENDING_404 || conn->state == STATE_404_SENT)
		response_code = "404 Not Found";
	else
		response_code = "500 Internal Server Error";

	/* Prepare the HTTP response header. */
	int header_len = snprintf(conn->send_buffer, BUFSIZ,
		"HTTP/1.1 %s\r\n"
		"\r\n",
		response_code);

	/* Update the length of the data to send. */
	conn->send_len = header_len;
}

static void connection_prepare_send_404(struct connection *conn)
{
	/* Prepare the connection buffer to send the 404 header. */
	conn->state = STATE_SENDING_404;
	connection_prepare_send_reply_header(conn);
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */
	const char *path = conn->request_path;

	/* Check if the path points to the static or dynamic folder */
	if (strncmp(path, "/static/", 8) == 0)
		return RESOURCE_TYPE_STATIC;
	else if (strncmp(path, "/dynamic/", 9) == 0)
		return RESOURCE_TYPE_DYNAMIC;

	return RESOURCE_TYPE_NONE;
}


struct connection *connection_create(int sockfd)
{
	// Allocate connection handler.
	struct connection *conn = malloc(sizeof(struct connection));

	DIE(conn == NULL, "malloc");

	conn->sockfd = sockfd;
	memset(conn->recv_buffer, 0, BUFSIZ);
	memset(conn->send_buffer, 0, BUFSIZ);

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* Start asynchronous operation (read from file).
	 * Used io_submit() for reading data asynchronously.
	 */
	if (conn->file_pos == 0) {
		if (io_setup(MAX_EVENTS, &conn->ctx) < 0) {
			/* Handle error. */
			perror("io_setup");
		}
	}

	// Create the eventfd.
	io_set_eventfd(&conn->iocb, conn->eventfd);

	/* Initialize the iocb for a read operation. */
	io_prep_pread(&conn->iocb, conn->fd, conn->send_buffer, BUFSIZ, conn->file_pos);
	conn->file_pos += BUFSIZ;

	/* Submit the iocb. */
	conn->piocb[0] = &conn->iocb;
	if (io_submit(conn->ctx, 1, conn->piocb) < 0) {
		/* Handle error. */
		return;
	}
}

void connection_remove(struct connection *conn)
{
	close(conn->sockfd);
	conn->state = STATE_CONNECTION_CLOSED;
	free(conn);
}

void handle_new_connection(void)
{
	static int sockfd;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;
	struct connection *conn;
	int rc;

	// accept new connection
	sockfd = accept(listenfd, (SSA *) &addr, &addrlen);
	DIE(sockfd < 0, "accept");

	// Set socket to be non-blocking.
	fcntl(sockfd, F_SETFL, O_NONBLOCK);

	// Instantiate new connection handler
	conn = connection_create(sockfd);

	// Add socket to epoll
	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	DIE(rc < 0, "w_epoll_add_in");

	// Initialize HTTP_REQUEST parser.
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
}

void receive_data(struct connection *conn)
{
	/* Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */
	ssize_t bytes_recv;
	char temp_buffer[BUFSIZ];
	char *dynamic_buffer = NULL;
	size_t total_bytes = 0;

	while ((bytes_recv = recv(conn->sockfd, temp_buffer, BUFSIZ, 0)) > 0) {
		/* Reallocate the dynamic buffer to hold the new data. */
		dynamic_buffer = realloc(dynamic_buffer, total_bytes + bytes_recv);
		if (!dynamic_buffer) {
			perror("realloc");
			return;
		}

		/* Copy the new data into the dynamic buffer. */
		memcpy(dynamic_buffer + total_bytes, temp_buffer, bytes_recv);
		total_bytes += bytes_recv;
	}

	memcpy(conn->recv_buffer, dynamic_buffer, total_bytes);
	conn->recv_len = total_bytes;
	conn->state = STATE_REQUEST_RECEIVED;
}

int connection_open_file(struct connection *conn)
{
	int fd;
	struct stat st;
	char *filename = conn->request_path + 1;

	/* Open the file. */
	fd = open(filename, O_RDONLY);
	if (fd < 0) {
		perror("open");
		return -1;
	}

	/* Get the file size. */
	if (fstat(fd, &st) < 0) {
		perror("fstat");
		close(fd);
		return -1;
	}

	/* Update the connection fields. */
	conn->fd = fd;
	conn->file_size = st.st_size;
	conn->file_pos = 0;

	return 0;
}

void connection_complete_async_io(struct connection *conn)
{
	struct io_event events[MAX_EVENTS];
	int num_events;

	/* Wait for the asynchronous read operation to complete. */
	num_events = io_getevents(conn->ctx, 1, MAX_EVENTS, events, NULL);

	if (num_events < 0) {
		/* Handle error. */
		perror("io_getevents");
		return;
	}

	/* The read operation completed successfully. */
	/* Prepare the socket for sending. */
	conn->send_len = events[0].res;
}

int parse_header(struct connection *conn)
{
	/* Use mostly null settings except for on_path callback. */
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};

	/* Parse the HTTP header and extract the file path. */
	conn->request_parser.data = conn;
	size_t parsed = http_parser_execute(&conn->request_parser, &settings_on_path, conn->recv_buffer, conn->recv_len);

	if (parsed != conn->recv_len)
		return -1;

	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	int fd;
	struct stat stat_buf;
	off_t offset = 0;
	ssize_t bytes_sent;
	ssize_t total_bytes_sent = 0;
	char *filename = conn->request_path + 1;

	/* Open the file for reading. */
	fd = open(filename, O_RDONLY);
	if (fd == -1) {
		perror("open");
		return STATE_404_SENT;
	}

	/* Get the size of the file. */
	if (fstat(fd, &stat_buf) == -1) {
		perror("fstat");
		close(fd);
		return STATE_404_SENT;
	}

	conn->send_len = stat_buf.st_size;

	/* Send the file in chunks. */
	while (conn->send_len > 0) {
		bytes_sent = sendfile(conn->sockfd, fd, &offset, conn->send_len);
		if (bytes_sent > 0) {
			total_bytes_sent += bytes_sent;
			conn->send_len -= bytes_sent;
		}
	}

	close(fd);
	return STATE_DATA_SENT;
}

int connection_send_data(struct connection *conn)
{
	/* Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */
	ssize_t bytes_sent = 0;
	ssize_t total_bytes_sent = 0;

	while (conn->send_len > 0) {
		bytes_sent = send(conn->sockfd, conn->send_buffer + total_bytes_sent, conn->send_len, 0);
		if (bytes_sent < 0) {
			/* An error occurred while sending data. */
			perror("send");
		} else {
			total_bytes_sent += bytes_sent;
			conn->send_len -= bytes_sent;
		}
	}

	// /* all done - remove out notification */
	int rc = w_epoll_update_ptr_in(epollfd, conn->sockfd, conn);

	DIE(rc < 0, "w_epoll_update_ptr_in");

	return bytes_sent;
}


void handle_input(struct connection *conn)
{
	/* Receive message on socket. */
	receive_data(conn);

	/* Parse the HTTP header. */
	if (parse_header(conn) < 0) {
		/* The header could not be parsed. */
		connection_prepare_send_404(conn);
		conn->state = STATE_SENDING_HEADER;
		connection_send_data(conn);
	} else {
		/* The header was parsed successfully. */
		conn->res_type = connection_get_resource_type(conn);
		if (conn->res_type == RESOURCE_TYPE_STATIC) {
			/* The resource is static. */
			conn->state = STATE_SENDING_HEADER;
			if (connection_open_file(conn) < 0) {
				connection_prepare_send_404(conn);
				/* Send the data. */
				connection_send_data(conn);
			} else {
				connection_prepare_send_reply_header(conn);
				/* Send the data. */
				connection_send_data(conn);
				connection_send_static(conn);
			}
		} else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
			/* The resource is dynamic. */
			conn->state = STATE_SENDING_DATA;
			conn->file_pos = 0;
			if (connection_open_file(conn) < 0) {
				connection_prepare_send_404(conn);
				/* Send the data. */
				connection_send_data(conn);
			} else {
				connection_prepare_send_reply_header(conn);

				/* Send the data. */
				while (conn->file_pos < BIGGER_BUFSIZE) {
					connection_start_async_io(conn);
					connection_complete_async_io(conn);
					if (conn->send_len == 0)
						break;
					connection_send_data(conn);
				}
			}
		} else {
			/* The resource does not exist. */
			connection_prepare_send_404(conn);
			conn->state = STATE_SENDING_HEADER;
			connection_send_data(conn);
		}
	}

	/* Close the connection. */
	connection_remove(conn);
}

void handle_client(uint32_t event, struct connection *conn)
{
	// Handle new client.
	if (event & EPOLLIN) {
		/* There is data to read. */
		handle_input(conn);
	}
}

int main(void)
{
	int rc;

	ctx = 0;

	// Create epoll file descriptor.
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "w_epoll_create");

	// Create listener socket.
	listenfd = tcp_create_listener(ECHO_LISTEN_PORT, DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener");

	// Add listener socket to epoll.
	rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "w_epoll_add_fd_in");

	dlog(LOG_INFO, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		// Wait for events.
		rc = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc < 0, "w_epoll_wait_infinite");

		// Handle events.
		if (rev.data.fd == listenfd) {
			dlog(LOG_DEBUG, "New connection\n");
			handle_new_connection();
		} else {
			handle_client(rev.events, rev.data.ptr);
		}
	}

	return 0;
}
