/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include "command_ids.h"
#include <memcached/protocol_binary.h>

#include <getopt.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

static void initialize_sockets(void)
{
#ifdef WIN32
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2,0), &wsaData) != 0) {
       fprintf(stderr, "Socket Initialization Error. Program aborted\n");
       exit(EXIT_FAILURE);
    }
#endif
}

/**
 * Try to connect to the server
 * @param host the name of the server
 * @param port the port to connect to
 * @return a socket descriptor connected to host:port for success, -1 otherwise
 */
static int connect_server(const char *hostname, const char *port)
{
    struct addrinfo *ai = NULL;
    struct addrinfo hints = { .ai_family = AF_UNSPEC,
                              .ai_protocol = IPPROTO_TCP,
                              .ai_socktype = SOCK_STREAM };

    if (getaddrinfo(hostname, port, &hints, &ai) != 0) {
        return -1;
    }
    int sock = -1;
    if ((sock = socket(ai->ai_family, ai->ai_socktype,
                       ai->ai_protocol)) != -1) {
        if (connect(sock, ai->ai_addr, ai->ai_addrlen) == -1) {
            close(sock);
            sock = -1;
        }
    }

    freeaddrinfo(ai);
    return sock;
}

/**
 * Send the chunk of data to the other side, retry if an error occurs
 * (or terminate the program if retry wouldn't help us)
 * @param sock socket to write data to
 * @param buf buffer to send
 * @param len length of data to send
 */
static void retry_send(int sock, const void* buf, size_t len)
{
    off_t offset = 0;
    const char* ptr = buf;

    do {
        size_t num_bytes = len - offset;
        ssize_t nw = send(sock, ptr + offset, num_bytes, 0);
        if (nw == -1) {
            if (errno != EINTR) {
                fprintf(stderr, "Failed to write: %s\n", strerror(errno));
                close(sock);
                exit(EXIT_FAILURE);
            }
        } else {
            offset += nw;
        }
    } while (offset < (off_t)len);
}

/**
 * Receive a fixed number of bytes from the socket.
 * (Terminate the program if we encounter a hard error...)
 * @param sock socket to receive data from
 * @param buf buffer to store data to
 * @param len length of data to receive
 */
static void retry_recv(int sock, void *buf, size_t len) {
    if (len == 0) {
        return;
    }
    off_t offset = 0;
    do {
        ssize_t nr = recv(sock, ((char*)buf) + offset, len - offset, 0);
        if (nr == -1) {
            if (errno != EINTR) {
                fprintf(stderr, "Failed to read: %s\n", strerror(errno));
                close(sock);
                exit(EXIT_FAILURE);
            }
        } else {
            if (nr == 0) {
                fprintf(stderr, "Connection closed\n");
                close(sock);
                exit(EXIT_FAILURE);
            }
            offset += nr;
        }
    } while (offset < (off_t)len);
}

static void read_result(int sock, const char *prefix)
{
    protocol_binary_response_no_extras response;
    retry_recv(sock, &response, sizeof(response.bytes));
    char *message = NULL;
    uint32_t bodylen = ntohl(response.message.header.response.bodylen);
    if (bodylen > 0) {
        message = malloc(bodylen + 1);
        if (message == NULL) {
            fprintf(stderr, "Internal error. Failed to allocate memory\n");
            exit(EXIT_FAILURE);
        }
        retry_recv(sock, message, bodylen);
        message[bodylen] = '\0';
    }

    uint16_t error = ntohs(response.message.header.response.status);
    if (error != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        fprintf(stderr, "%s: ", prefix);
        switch (error) {
        case PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED:
            fprintf(stderr, "Not supported");
            break;
        case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
            fprintf(stderr, "Not found");
            break;
        case PROTOCOL_BINARY_RESPONSE_EINTERNAL:
            fprintf(stderr, "Internal error");
            break;
        default:
            fprintf(stderr, "Unknown error %x", error);
        }
        fprintf(stderr, "\n");
        if (message) {
            fprintf(stderr, "  %s\n", message);
        }
        exit(EXIT_FAILURE);
    }
}

/**
 * Request the server to start backup from a file
 * @param sock socket connected to the server
 * @param file the name of the file to restore
 */
static void start_restore(int sock, const char *file)
{
    uint16_t keylen = (uint16_t)strlen(file);
    protocol_binary_request_stats request = {
        .message.header.request = {
            .magic = PROTOCOL_BINARY_REQ,
            .opcode = CMD_RESTORE_FILE,
            .keylen = htons(keylen),
            .bodylen = htonl(keylen)
        }
    };

    retry_send(sock, &request, sizeof(request));
    retry_send(sock, file, keylen);
    read_result(sock, "Failed to start restore");
    fprintf(stdout, "Restore successfully initiated\n");
}

/**
 * Tell the server that we're done restoring it
 * @param sock socket connected to the server
 */
static void finalize_restore(int sock)
{
    protocol_binary_request_stats request = {
        .message.header.request = {
            .magic = PROTOCOL_BINARY_REQ,
            .opcode = CMD_RESTORE_COMPLETE,
        }
    };

    retry_send(sock, &request, sizeof(request));
    read_result(sock, "Failed to exit restore mode");
    fprintf(stdout, "Server successfully left restore mode\n");
}

/**
 * Print the key value pair
 * @param key key to print
 * @param keylen length of key to print
 * @param val value to print
 * @param vallen length of value
 */
static void print(const char *key, int keylen, const char *val, int vallen) {
    fputs("STAT ", stdout);
    fwrite(key, keylen, 1, stdout);
    fputs(" ", stdout);
    fwrite(val, vallen, 1, stdout);
    fputs("\n", stdout);
    fflush(stdout);
}

/**
 * Request a stat from the server
 * @param sock socket connected to the server
 * @param key the name of the stat to receive (NULL == ALL)
 */
static void request_stat(int sock, const char *key)
{
    uint32_t buffsize = 0;
    char *buffer = NULL;
    uint16_t keylen = 0;
    if (key != NULL) {
        keylen = (uint16_t)strlen(key);
    }

    protocol_binary_request_stats request = {
        .message.header.request = {
            .magic = PROTOCOL_BINARY_REQ,
            .opcode = PROTOCOL_BINARY_CMD_STAT,
            .keylen = htons(keylen),
            .bodylen = htonl(keylen)
        }
    };

    retry_send(sock, &request, sizeof(request));
    if (keylen > 0) {
        retry_send(sock, key, keylen);
    }

    protocol_binary_response_no_extras response;
    do {
        retry_recv(sock, &response, sizeof(response.bytes));
        if (response.message.header.response.keylen != 0) {
            uint32_t vallen = ntohl(response.message.header.response.bodylen);
            keylen = ntohs(response.message.header.response.keylen);
            if (vallen > buffsize) {
                if ((buffer = realloc(buffer, vallen)) == NULL) {
                    fprintf(stderr, "Failed to allocate memory\n");
                    exit(EXIT_FAILURE);
                }
                buffsize = vallen;
            }
            retry_recv(sock, buffer, vallen);
            print(buffer, keylen, buffer + keylen, vallen - keylen);
        }
    } while (response.message.header.response.keylen != 0);
}

/**
 * Program entry point. Connect to a memcached server and use the binary
 * protocol to retrieve a given set of stats.
 *
 * @param argc argument count
 * @param argv argument vector
 * @return 0 if success, error code otherwise
 */
int main(int argc, char **argv)
{
    int cmd;
    const char * const default_ports[] = { "memcache", "11210", NULL };
    const char *port = NULL;
    const char *host = NULL;
    char *ptr;
    const char *file = NULL;
    bool finalize = false;
    bool status = false;
    /* Initialize the socket subsystem */
    initialize_sockets();

    while ((cmd = getopt(argc, argv, "h:p:f:cs?")) != EOF) {
        switch (cmd) {
        case 's' :
            status = true;
            break;
        case 'c' :
            finalize = true;
            break;
        case 'f' :
            file = optarg;
            break;

        case 'h' :
            host = optarg;
            ptr = strchr(optarg, ':');
            if (ptr != NULL) {
                *ptr = '\0';
                port = ptr + 1;
            }
            break;
        case 'p':
            port = optarg;
            break;
        default:
            fprintf(stderr,
                    "Usage %s [-h host[:port]] [-p port] [-f file] [-c] [-s]\n", argv[0]);
            return 1;
        }
    }

    if (file) {
        if (access(file, F_OK) == -1) {
            fprintf(stderr, "File not found: [%s]\n", file);
            return 2;
        }

        char path[2048];
        if (realpath(file, path) == NULL) {
            fprintf(stderr, "Failed to resolve the absolute path for: [%s]\n", file);
            return 2;
        }
        if ((file = strdup(path)) == NULL) {
            fprintf(stderr, "Internal error. failed to allocate memory");
            return 2;
        }
    } else if (!finalize && !status) {
        fprintf(stderr, "You need to use either -f, -c or -s\n");
        return 1;
    }

    if (host == NULL) {
        host = "localhost";
    }

    int sock = -1;
    if (port == NULL) {
        int ii = 0;
        do {
            port = default_ports[ii++];
            sock = connect_server(host, port);
        } while (sock == -1 && default_ports[ii] != NULL);
    } else {
        sock = connect_server(host, port);
    }

    if (sock == -1) {
        fprintf(stderr, "Failed to connect to membase server (%s:%s): %s\n",
                host, port, strerror(errno));
        return 1;
    }

    // @todo add SASL auth if it's specified

    if (file) {
        start_restore(sock, file);
    }

    if (status) {
        int num = 1;
        int sleeptime = 1;

        if (optind < argc) {
            num = atoi(argv[optind]);
            ++optind;
            if (optind < argc) {
                sleeptime = atoi(argv[optind]);
            }
        }

        if (num == 0) {
            num = 1;
        }
        if (sleeptime == 0) {
            sleeptime = 1;
        }

        while (num > 0) {
            request_stat(sock, "restore");
            if (--num > 0) {
                usleep(sleeptime * 1000);
            }
        }
    }

    if (finalize) {
        finalize_restore(sock);
    }

    close(sock);

    return 0;
}
