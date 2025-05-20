#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/mman.h>
#include <liburing.h>
#include <pthread.h>
#include <stdatomic.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#define ONE_MB (1024 * 1024)  // 1MB size
#define BUFFER_SIZE 1024      // Standard buffer size
#define BUFFER_COUNT 4        // Number of buffers in the ring
#define QUEUE_DEPTH 64        // io_uring queue depth

// Atomic counter for tracking received data
atomic_size_t data_received = 0;
atomic_bool task_completed = false;

struct buf_data {
    void *addr;
    uint16_t bid;
    uint32_t len;
};

// Structure to hold the buffer ring information
struct buf_ring_data {
    struct io_uring_buf_ring *buf_ring;
    void *buffer_memory;
    uint16_t ring_entries;
    uint32_t buf_size;
};

// Create a TCP socket server-client pair for testing
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

// Setup and initialize the buffer ring
struct buf_ring_data setup_buf_ring(struct io_uring *ring, uint16_t entries, uint32_t buf_size, int bgid) {
    struct buf_ring_data result;
    result.ring_entries = entries;
    result.buf_size = buf_size;
    
    // Allocate memory for buffers (aligned to page size)
    size_t total_size = entries * buf_size;
    int page_size = sysconf(_SC_PAGESIZE);
    size_t aligned_size = (total_size + page_size - 1) & ~(page_size - 1);
    
    void *buffer_memory = mmap(NULL, aligned_size, PROT_READ | PROT_WRITE, 
                              MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    assert(buffer_memory != MAP_FAILED);
    result.buffer_memory = buffer_memory;
    
    // Allocate and setup buf_ring
	void *mapped;
    struct io_uring_buf_ring *buf_ring;
    int ring_size = entries * sizeof(struct io_uring_buf);
    // Ensure ring size is a multiple of page size
    ring_size = (ring_size + page_size - 1) & ~(page_size - 1);
    
    mapped = mmap(NULL, ring_size, PROT_READ | PROT_WRITE, 
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    assert(mapped != MAP_FAILED);

	buf_ring = (struct io_uring_buf_ring *)mapped;
    
    io_uring_buf_ring_init(buf_ring);
    result.buf_ring = buf_ring;

	struct io_uring_buf_reg reg = { 
		.ring_addr = (unsigned long)buf_ring,
		.ring_entries = entries,
		.bgid = 0 
	};
    
    // Register the buf_ring with io_uring
    int ret = io_uring_register_buf_ring(ring, &reg, 0);
    assert(ret == 0);
    
    // Add buffers to the ring
    for (int i = 0; i < entries; i++) {
        void *buf_addr = (char*)buffer_memory + (i * buf_size);
        io_uring_buf_ring_add(buf_ring, buf_addr, buf_size, i, 
                             io_uring_buf_ring_mask(entries), i);
    }
    
    // Advance the ring to make buffers available
    io_uring_buf_ring_advance(buf_ring, entries);
    
    return result;
}

// Verify the received data matches what was sent
void verify_received_buffer(struct buf_data *buf, uint8_t *expected_data_start) {
    uint8_t *data = buf->addr;
    for (uint32_t i = 0; i < buf->len; i++) {
        uint8_t expected = *(expected_data_start + i);
        fprintf(stderr, "%u == %u\n", data[i], expected);
        assert(data[i] == expected);
    }
}

// Process a completed receive operation
void process_completion(struct io_uring_cqe *cqe, struct buf_ring_data *br_data, uint8_t **current_expect) {
    fprintf(stderr, "\nhandled completion: bid: %d, res: %d, has_more: %d\n", cqe->flags >> IORING_CQE_BUFFER_SHIFT, cqe->res, cqe->flags & IORING_CQE_F_MORE);
    if (cqe->res <= 0) {
        // Handle error or EOF
        if (cqe->res == 0) {
            fprintf(stderr, "EOF reached\n");
        } else {
            // fprintf(stderr, "Error in receive: %d\n", cqe->res);
        }
        return;
    }
    
    // Get buffer ID and length from the completion
    uint16_t bid = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
    uint32_t len = cqe->res;
    
    // Calculate buffer address
    void *buffer_addr = (char*)br_data->buffer_memory + (bid * br_data->buf_size);
    
    // Create buf_data structure
    struct buf_data buf = {
        .addr = buffer_addr,
        .bid = bid,
        .len = len
    };
    
    // Update received count
    atomic_fetch_add(&data_received, len);
    
    // Print buffer information
    fprintf(stderr, "bid: [%u]/(%u)/(%p): \n", bid, len, buffer_addr);
    for (int i = 0; i < 10 && i < len; i++) {
        fprintf(stderr, "%u ", ((uint8_t*)buffer_addr)[i]);
    }
    fprintf(stderr, "...\n");
    
    // Verify data matches expected values
    verify_received_buffer(&buf, *current_expect);
    *current_expect += len; // Update expect pointer
    
    // Recycle buffer by adding it back to the ring
    io_uring_buf_ring_add(br_data->buf_ring, buffer_addr, br_data->buf_size, 
                         bid, io_uring_buf_ring_mask(br_data->ring_entries), 0);
    int advance_buf = (cqe->res + br_data->buf_size - 1) / br_data->buf_size; // Ceiling division
    fprintf(stderr, "io_uring_buf_ring_advance: %d\n", advance_buf);
    io_uring_buf_ring_advance(br_data->buf_ring, advance_buf);
}

// Main test function
int test_recv_multi_large_packet_isolate_ring() {
	fprintf(stderr, "test_recv_multi_large_packet_isolate_ring\n");
    // Setup io_uring
    struct io_uring ring;
    struct io_uring_params params = {0};
    int ret = io_uring_queue_init_params(QUEUE_DEPTH, &ring, &params);
    assert(ret == 0);
    
    // Setup buffer ring
    struct buf_ring_data br_data = setup_buf_ring(&ring, BUFFER_COUNT, BUFFER_SIZE, 0);
    
    // Create socket pair
    int socket_fds[2];
    ret = create_socket_pair(socket_fds);
	fprintf(stderr, "create_socket_pair\n");
    assert(ret == 0);
    int receiver_fd = socket_fds[0];
    int sender_fd = socket_fds[1];
    
    // Create test data (1MB)
    uint8_t *test_data = malloc(ONE_MB);
	fprintf(stderr, "malloc(ONE_MB)\n");
    assert(test_data != NULL);
    for (int i = 0; i < ONE_MB; i++) {
        test_data[i] = i % 256;
    }
	fprintf(stderr, "prepared test_data\n");
    
    // Write data to socket
	fprintf(stderr, "Write data to socket\n");
    size_t bytes_sent = 0;
    while (bytes_sent < ONE_MB) {
		fprintf(stderr, "try send\n");
        ssize_t sent = write(sender_fd, test_data + bytes_sent, ONE_MB - bytes_sent);
        assert(sent > 0);
		fprintf(stderr, "send: %ld bytes\n", sent);
        bytes_sent += sent;
    }
    
    // Close sender to signal EOF
    close(sender_fd);
    
    // Start receiving data
    uint8_t *current_expect = test_data; // Track where we are in the expected data
    
    for (int i = 0; i < BUFFER_COUNT; i++) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        assert(sqe != NULL);
        
        // Setup receive operation with buffer ring
        io_uring_prep_recv_multishot(sqe, receiver_fd, NULL, 0, 0);
        sqe->flags |= IOSQE_BUFFER_SELECT;
        sqe->ioprio |= IORING_RECVSEND_BUNDLE;
        sqe->buf_group = 0; // Buffer group ID
        
        ret = io_uring_submit(&ring);
        assert(ret == 1);
    }
    
    // Process completions
    struct io_uring_cqe *cqe;
    int poll_count = 0;
    
    // Loop until all data is received or an error occurs
    while (atomic_load(&data_received) < ONE_MB && poll_count < 5000) {
        ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret == 0) {
            process_completion(cqe, &br_data, &current_expect);
            if (!(cqe->flags & IORING_CQE_F_MORE) && !(cqe->res)) {
                io_uring_cq_advance(&ring, 1);
                break;
            }

            // If this wasn't the last buffer (not IORING_CQE_F_MORE), submit a new request
            if (!(cqe->flags & IORING_CQE_F_MORE) && cqe->res) {
                struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
                assert(sqe != NULL);
                io_uring_prep_recv_multishot(sqe, receiver_fd, NULL, 0, 0);
                sqe->flags |= IOSQE_BUFFER_SELECT;
                sqe->ioprio |= IORING_RECVSEND_BUNDLE;
                sqe->buf_group = 0;
                ret = io_uring_submit(&ring);
                fprintf(stderr, "repanwed");
                assert(ret == 1);
            }
            io_uring_cq_advance(&ring, 1);
        }
        
        // Periodically log progress
        if (poll_count % 1000 == 0) {
            size_t current = atomic_load(&data_received);
            fprintf(stderr, "[Main] After %d iterations: received %zu bytes\n", poll_count, current);
        }
        
        poll_count++;
    }
    
    // Check completion and verify data
    size_t total_received = atomic_load(&data_received);
    fprintf(stderr, "Total received: %zu bytes, expected: %d bytes\n", total_received, ONE_MB);
    assert(total_received == ONE_MB);
    
    // Cleanup
    close(receiver_fd);
    io_uring_queue_exit(&ring);
    munmap(br_data.buffer_memory, BUFFER_COUNT * BUFFER_SIZE);
    munmap(br_data.buf_ring, BUFFER_COUNT * sizeof(struct io_uring_buf));
    free(test_data);
    
    return 0;
}

int main(int argc, char *argv[]) {
    printf("%s\n", "hi");
    return test_recv_multi_large_packet_isolate_ring();
}

