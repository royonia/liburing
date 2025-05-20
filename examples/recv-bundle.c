/*
 * recv-bundle.c - Example demonstrating io_uring's IORING_RECVSEND_BUNDLE feature
 *
 * This example shows how to use io_uring's buffer ring and multishot recv with
 * IORING_RECVSEND_BUNDLE to efficiently receive large amounts of data (1MB).
 * It demonstrates setting up a local socket pair, sending test data, and
 * receiving it using a buffer ring configuration.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <liburing.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

/* Configuration constants */
#define ONE_MB      (1024 * 1024)  /* Size of test data (1MB) */
#define BUFFER_SIZE 1024           /* Size of each buffer in bytes */

#define BUFFER_COUNT 4             /* Number of buffers in the ring */
#define QUEUE_DEPTH 16             /* io_uring queue depth */

#define min(a, b) (((a) < (b)) ? (a) : (b))

/* Get the system page size */
#define PAGE_SIZE sysconf(_SC_PAGESIZE)
#define IS_PAGE_ALIGNED(addr) (((uintptr_t)(addr) & (PAGE_SIZE - 1)) == 0)

/* Macros to check alignment for different data types */
#define IS_ALIGNED(addr, bytes) (((uintptr_t)(addr) & (bytes - 1)) == 0)
#define IS_2BYTE_ALIGNED(addr) IS_ALIGNED(addr, 2)
#define IS_4BYTE_ALIGNED(addr) IS_ALIGNED(addr, 4)
#define IS_8BYTE_ALIGNED(addr) IS_ALIGNED(addr, 8)
#define IS_16BYTE_ALIGNED(addr) IS_ALIGNED(addr, 16)

/* Global state tracking */
size_t data_received = 0;   /* Tracks total bytes received */

/* Function prototypes */
void write_all(int fd, const void *data, size_t size);

/**
 * Buffer data structure
 * Contains information about a buffer from the ring
 */
struct buf_data {
    void     *addr;  /* Buffer memory address */
    uint16_t  bid;   /* Buffer ID within the ring */
    uint32_t  len;   /* Length of valid data in the buffer */
};

/**
 * Buffer ring data structure
 * Holds information needed to manage a buffer ring
 */
struct buf_ring_data {
    struct io_uring_buf_ring *buf_ring;  /* The io_uring buffer ring */
    void                     *buffer_memory;  /* Memory for all buffers */
    uint16_t                  ring_entries;   /* Number of entries in the ring */
    uint32_t                  buf_size;       /* Size of each buffer */
};

/**
 * Creates a connected TCP socket pair for testing
 *
 * This function sets up a TCP server-client socket pair on localhost (127.0.0.1)
 * with a dynamically assigned port. It creates, connects, and returns both sockets.
 *
 * @param fds Array to store socket file descriptors:
 *            fds[0]: Server (receiver) socket
 *            fds[1]: Client (sender) socket
 *
 * @return 0 on success, -1 on failure
 */
int create_socket_pair(int fds[2]) {
    int server_fd, client_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t addr_len = sizeof(struct sockaddr_in);
    
    // Create server socket
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Server socket creation failed");
        return -1;
    }
    
    // Set socket options for reuse
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt failed");
        close(server_fd);
        return -1;
    }
    
    // Configure server address
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    server_addr.sin_port = 0;  // Let the OS assign a port
    
    // Bind the server socket
    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        close(server_fd);
        return -1;
    }
    
    // Get the assigned port
    if (getsockname(server_fd, (struct sockaddr *)&server_addr, &addr_len) < 0) {
        perror("getsockname failed");
        close(server_fd);
        return -1;
    }
    
    // Listen for connections
    if (listen(server_fd, 1) < 0) {
        perror("Listen failed");
        close(server_fd);
        return -1;
    }
    
    // Create client socket
    client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0) {
        perror("Client socket creation failed");
        close(server_fd);
        return -1;
    }
    
    // Configure client to connect to the server
    memset(&client_addr, 0, sizeof(client_addr));
    client_addr.sin_family = AF_INET;
    client_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    client_addr.sin_port = server_addr.sin_port;  // Use the port assigned to the server
    
    // Connect to server
    if (connect(client_fd, (struct sockaddr *)&client_addr, sizeof(client_addr)) < 0) {
        perror("Connection failed");
        close(client_fd);
        close(server_fd);
        return -1;
    }
    
    // Accept connection on server side
    int accepted_fd = accept(server_fd, (struct sockaddr *)&client_addr, &addr_len);
    if (accepted_fd < 0) {
        perror("Accept failed");
        close(client_fd);
        close(server_fd);
        return -1;
    }
    
    // Close the original server socket as we no longer need it
    close(server_fd);
    
    // Set the server and client file descriptors
    fds[0] = accepted_fd;  // Server side (receiver)
    fds[1] = client_fd;    // Client side (sender)
    
    return 0;
}

/**
 * Sets up and initializes the buffer ring for io_uring
 *
 * This function allocates memory for the buffer ring and all individual buffers,
 * initializes the buffer ring, registers it with io_uring, and adds all buffers
 * to the ring.
 *
 * @param ring      Pointer to the io_uring instance
 * @param entries   Number of buffer entries to create
 * @param buf_size  Size of each buffer in bytes
 * @param bgid      Buffer group ID to use
 *
 * @return A configured buf_ring_data structure
 */
struct buf_ring_data setup_buf_ring(struct io_uring *ring, uint16_t entries, uint32_t buf_size, int bgid) {
    struct buf_ring_data result;
    result.ring_entries = entries;
    result.buf_size = buf_size;
    
    /* Allocate page-aligned memory for all buffers */
    size_t total_size = entries * buf_size;
    int page_size = sysconf(_SC_PAGESIZE);
    size_t aligned_size = (total_size + page_size - 1) & ~(page_size - 1);
    
    void *buffer_memory = mmap(NULL, aligned_size, PROT_READ | PROT_WRITE, 
                              MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    assert(buffer_memory != MAP_FAILED);
    
    /* Verify buffer memory is page-aligned as guaranteed by mmap */
    assert(IS_PAGE_ALIGNED(buffer_memory));
    result.buffer_memory = buffer_memory;
    
    /* Allocate and setup buffer ring with page alignment */
	void *mapped;
    struct io_uring_buf_ring *buf_ring;
    int ring_size = entries * sizeof(struct io_uring_buf);
    
    /* Round up ring size to page boundary */
    ring_size = (ring_size + page_size - 1) & ~(page_size - 1);
    
    mapped = mmap(NULL, ring_size, PROT_READ | PROT_WRITE, 
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    assert(mapped != MAP_FAILED);

	buf_ring = (struct io_uring_buf_ring *)mapped;
    
    /* Initialize the buffer ring structure */
    io_uring_buf_ring_init(buf_ring);
    result.buf_ring = buf_ring;

	/* Prepare registration parameters */
	struct io_uring_buf_reg reg = { 
		.ring_addr = (unsigned long)buf_ring,
		.ring_entries = entries,
		.bgid = 0 
	};
    
    /* Register the buffer ring with io_uring */
    int ret = io_uring_register_buf_ring(ring, &reg, 0);
    assert(ret == 0);
    
    /* Add all individual buffers to the ring */
    for (int i = 0; i < entries; i++) {
        void *buf_addr = (char*)buffer_memory + (i * buf_size);
        io_uring_buf_ring_add(buf_ring, buf_addr, buf_size, i, 
                             io_uring_buf_ring_mask(entries), i);
    }
    
    /* Make all buffers available by advancing the tail pointer */
    io_uring_buf_ring_advance(buf_ring, entries);
    
    return result;
}

/**
 * Verifies that received buffer data matches expected data
 *
 * This function compares each byte of received data against the expected data
 * and asserts if any mismatch is found. It also prints the comparison for debugging.
 *
 * @param buf                 Pointer to buffer containing received data
 * @param expected_data_start Pointer to start of expected data for comparison
 */
void verify_received_buffer(struct buf_data *buf, uint8_t *expected_data_start) {
    uint8_t *data = buf->addr;
    for (uint32_t i = 0; i < buf->len; i++) {
        uint8_t expected = *(expected_data_start + i);
        // fprintf(stderr, "%u == %u\n", data[i], expected);
        assert(data[i] == expected);
    }
}

/**
 * Processes a completed io_uring receive operation
 *
 * This function handles the completion queue entry by:
 * 1. Extracting the buffer ID and received data from the CQE
 * 2. Updating the global data received counter
 * 3. Verifying the data matches the expected pattern
 * 4. Recycling the buffer back to the buffer ring
 *
 * @param cqe             Completion queue entry to process
 * @param br_data         Buffer ring data structure
 * @param current_expect  Pointer to current position in expected data (updated by this function)
 */
void process_completion(struct io_uring_cqe *cqe, struct buf_ring_data *br_data, uint8_t **current_expect) {
    fprintf(stderr, "\nhandled completion: bid: %d, res: %d, has_more: %d\n", cqe->flags >> IORING_CQE_BUFFER_SHIFT, cqe->res, cqe->flags & IORING_CQE_F_MORE);
    if (cqe->res <= 0) {
        /* Handle error or EOF condition */
        if (cqe->res == 0) {
            fprintf(stderr, "EOF reached\n");
        } else {
            /* Uncomment for error debugging */
            /* fprintf(stderr, "Error in receive: %d\n", cqe->res); */
        }
        return;
    }
    
    /* Extract buffer ID and data length from completion */
    uint16_t bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
    uint32_t total_len = cqe->res;

    uint32_t nr_packet = 0;
    while (total_len) {
        nr_packet += 1;
        uint32_t this_len = min(BUFFER_SIZE, total_len);
        /* should never get a len large then bundled buffer size */
        assert(this_len <= BUFFER_SIZE);

        /* Calculate address of this buffer in our memory pool with proper alignment */
        void *buffer_addr = (uint8_t*)br_data->buffer_memory + (bid * BUFFER_SIZE);
        
        /* Verify buffer alignment for debugging */
        if (!IS_8BYTE_ALIGNED(buffer_addr)) {
            fprintf(stderr, "WARNING: Buffer address %p is not page aligned\n", buffer_addr);
        }

        /* Prepare buffer data structure for verification */
        struct buf_data buf = {
            .addr = buffer_addr,
            .bid = bid,
            .len = this_len
        };
        fprintf(stderr, "read buf[%d] len=%d\n", buf.bid, buf.len);

        /* Update global counter of total bytes received */
        data_received += this_len;

        /* Log buffer information for debugging */
        fprintf(stderr, "bid: [%u]/(%u)/(%p): \n", bid, this_len, buffer_addr);
        for (int i = 0; i < 10 && i < this_len; i++) {
            fprintf(stderr, "%u ", ((uint8_t*)buffer_addr)[i]);
        }
        fprintf(stderr, "...\n");

        /* Verify received data against expected pattern */
        verify_received_buffer(&buf, *current_expect);
        *current_expect += this_len; /* Move expected pointer forward */
    
        /* rearm the buffer */
        fprintf(stderr, "rearming buf[%d]\n", bid);
        io_uring_buf_ring_add(br_data->buf_ring, buffer_addr, BUFFER_SIZE, 
                bid, io_uring_buf_ring_mask(br_data->ring_entries), 0);

        /* Calculate next buffer id */
        bid = (bid + 1) & (BUFFER_COUNT - 1);
        total_len -= this_len;
        io_uring_buf_ring_advance(br_data->buf_ring, 1);
    }
    // if (nr_packet) {
    //     fprintf(stderr, "io_uring_buf_ring_advance: %d\n", nr_packet);
    //     io_uring_buf_ring_advance(br_data->buf_ring, nr_packet);
    // }
}

/**
 * Main test function for io_uring bundle receive mechanism
 *
 * This function demonstrates the complete flow of using io_uring's buffer ring
 * and multishot receive with IORING_RECVSEND_BUNDLE flag. It performs these steps:
 * 1. Set up an io_uring instance and buffer ring
 * 2. Create a TCP socket pair for testing
 * 3. Send 1MB of pattern data over the socket
 * 4. Receive and verify the data using io_uring operations
 *
 * @return 0 on success, non-zero on failure
 */
int test_recv_multi_large_packet_isolate_ring() {
	fprintf(stderr, "test_recv_multi_large_packet_isolate_ring\n");
    
    /* Initialize io_uring with parameters */
    struct io_uring ring;
    struct io_uring_params params = {0};
    int ret = io_uring_queue_init_params(QUEUE_DEPTH, &ring, &params);
    assert(ret == 0);
    
    /* Set up the buffer ring for receiving data */
    struct buf_ring_data br_data = setup_buf_ring(&ring, BUFFER_COUNT, BUFFER_SIZE, 0);
    
    /* Create socket pair for local communication testing */
    int socket_fds[2];
    ret = create_socket_pair(socket_fds);
    assert(ret == 0);
    int receiver_fd = socket_fds[0];
    int sender_fd = socket_fds[1];
    
    /* Allocate and initialize test data with pattern */
    uint8_t *test_data = malloc(ONE_MB);
    assert(test_data != NULL);
    for (int i = 0; i < ONE_MB; i++) {
        test_data[i] = i % 256;  /* Create repeating pattern */
    }
    
    /* Send test data through the socket */
    write_all(sender_fd, test_data, ONE_MB);
    fprintf(stderr, "sent %d bytes", ONE_MB);
    
    /* Close sender side to signal EOF to receiver */
    close(sender_fd);
    
    /* Initialize pointer to track our position in expected data */
    uint8_t *current_expect = test_data;
    
    /* Submit initial multishot receive operations with buffer selection */
    for (int i = 0; i < BUFFER_COUNT; i++) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        assert(sqe != NULL);
        
        /* Configure io_uring receive operation with buffer ring and bundle flag */
        io_uring_prep_recv_multishot(sqe, receiver_fd, NULL, 0, 0);
        sqe->flags |= IOSQE_BUFFER_SELECT;  /* Use buffer selection */
        sqe->ioprio |= IORING_RECVSEND_BUNDLE;  /* Enable bundle mode */
        sqe->buf_group = 0;  /* Use buffer group 0 */
        
        ret = io_uring_submit(&ring);
        assert(ret == 1);
    }
    
    /* Process completions from io_uring */
    struct io_uring_cqe *cqe;
    int poll_count = 0;
    
    /* Loop until we've received all data or exceed maximum iterations */
    while (data_received < ONE_MB && poll_count < 5000) {
        /* Wait for a completion event */
        ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret == 0) {
            /* Process this completion */
            process_completion(cqe, &br_data, &current_expect);
            
            /* Check for EOF (no more data and no more expected) */
            if (!(cqe->flags & IORING_CQE_F_MORE) && !(cqe->res)) {
                io_uring_cq_advance(&ring, 1);
                break;  /* Exit loop on EOF */
            }

            /* Respawn recv request if needed (when this one is done but no EOF) */
            if (!(cqe->flags & IORING_CQE_F_MORE) && cqe->res) {
                /* Get a submission queue entry */
                struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
                assert(sqe != NULL);
                
                /* Set up another multishot receive with same parameters */
                io_uring_prep_recv_multishot(sqe, receiver_fd, NULL, 0, 0);
                sqe->flags |= IOSQE_BUFFER_SELECT;
                sqe->ioprio |= IORING_RECVSEND_BUNDLE;
                sqe->buf_group = 0;
                
                /* Submit the new request */
                ret = io_uring_submit(&ring);
                fprintf(stderr, "respawned\n");  /* Log respawn */
                assert(ret == 1);
            }
            
            /* Mark completion as processed */
            io_uring_cq_advance(&ring, 1);
        }
        
        /* Periodically log progress (every 1000 iterations) */
        if (poll_count % 1000 == 0) {
            fprintf(stderr, "[Main] After %d iterations: received %zu bytes\n", poll_count, data_received);
        }
        
        poll_count++;
    }
    
    /* Verify we received all expected data */
    size_t total_received = data_received;
    fprintf(stderr, "Total received: %zu bytes, expected: %d bytes\n", total_received, ONE_MB);
    assert(total_received == ONE_MB);  /* Test fails if we didn't receive all data */
    
    /* Clean up all allocated resources */
    close(receiver_fd);                /* Close socket */
    io_uring_queue_exit(&ring);       /* Clean up io_uring */
    
    /* Free memory resources */
    munmap(br_data.buffer_memory, BUFFER_COUNT * BUFFER_SIZE);
    munmap(br_data.buf_ring, BUFFER_COUNT * sizeof(struct io_uring_buf));
    free(test_data);
    
    fprintf(stderr, "Test completed successfully\n");
    return 0;
}

/**
 * Writes all the data to the specified file descriptor
 *
 * This function ensures that all data is written, handling partial writes
 * by making repeated calls to write() until all bytes are sent.
 *
 * @param fd File descriptor to write to
 * @param data Pointer to the data buffer to write
 * @param size Number of bytes to write
 */
void write_all(int fd, const void *data, size_t size) {
    const uint8_t *buf = data;
    size_t bytes_sent = 0;
    
    /* Continue until all data is sent */
    while (bytes_sent < size) {
        ssize_t sent = write(fd, buf + bytes_sent, size - bytes_sent);
        assert(sent > 0);  /* Ensure write succeeded */
        bytes_sent += sent;
    } 
}

/**
 * Main entry point
 *
 * Simply prints a message and runs the test function.
 *
 * @param argc Command line argument count (unused)
 * @param argv Command line arguments (unused)
 * @return 0 on success, non-zero on failure
 * @return 0 on success, non-zero on failure
 */
int main(int argc, char *argv[]) {
    return test_recv_multi_large_packet_isolate_ring();
}

