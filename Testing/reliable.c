
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <stddef.h>
#include <assert.h>
#include <poll.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>

#include "rlib.h"

typedef struct packet_node {
  packet_t pkt;
  long long timestamp_ms;
  int retries;
  struct packet_node *next;
} packet_node_t;

struct reliable_state {
  rel_t *next;			/* Linked list for traversing all connections */
  rel_t **prev;
  conn_t *c;			/* This is the connection object */

  /* Add your own data fields below this */
  int window;
  int timeout;

  uint32_t unacked_base;
  uint32_t next_seqno;
  packet_node_t *tx_buffer;

  uint32_t expected_seqno;
  packet_node_t *rx_buffer;

  int eof_read;
  int eof_received;

  int closing;
  long long close_ts;

};

rel_t *rel_list;

long long get_time_ms() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

void send_ack(rel_t *r) {
  packet_t ack;
  memset(&ack, 0, sizeof(ack));
  ack.len = htons(8);
  ack.ackno = htonl(r->expected_seqno);
  ack.cksum = cksum(&ack, 8);
  conn_sendpkt(r->c, &ack, 8);
}

void check_destroy(rel_t *r) {
  if (r->closing) return;
  if (r->eof_read && r->eof_received && r->tx_buffer == NULL) {
    r->closing = 1;
    r->close_ts = get_time_ms();
  }
}



/* Creates a new reliable protocol session, returns NULL on failure.
 * Exactly one of c and ss should be NULL.  (ss is NULL when called
 * from rlib.c, while c is NULL when this function is called from
 * rel_demux.) */
rel_t *rel_create (conn_t *c, const struct sockaddr_storage *ss, const struct config_common *cc){
  rel_t *r;
 
  r = xmalloc (sizeof (*r));
  memset (r, 0, sizeof (*r));
 
  if (!c) {
    c = conn_create (r, ss);
    if (!c) {
      free (r);
      return NULL;
    }
  }
 
  r->c = c;
  r->next = rel_list;
  r->prev = &rel_list;
  if (rel_list) rel_list->prev = &r->next;
  rel_list = r;
 
  /* Do any other initialization you need here */
 
  r->window = cc->window;
  r->timeout = cc->timeout;
  r->unacked_base = 1;
  r->next_seqno = 1;
  r->expected_seqno = 1;
 
  return r;
}

void rel_destroy (rel_t *r) {
  if (r->next) r->next->prev = r->prev;
  *r->prev = r->next;
  conn_destroy (r->c);
 
  /* Free any other allocated memory here */
  packet_node_t *curr = r->tx_buffer;
  while (curr) {
    packet_node_t *tmp = curr;
    curr = curr->next;
    free(tmp);
  }
 
  curr = r->rx_buffer;
  while (curr) {
    packet_node_t *tmp = curr;
    curr = curr->next;
    free(tmp);
  }
  free(r);
}


/* This function only gets called when the process is running as a
 * server and must handle connections from multiple clients.  You have
 * to look up the rel_t structure based on the address in the
 * sockaddr_storage passed in.  If this is a new connection (sequence
 * number 1), you will need to allocate a new conn_t using rel_create
 * ().  (Pass rel_create NULL for the conn_t, so it will know to
 * allocate a new connection.)
 */
void rel_demux (const struct config_common *cc, const struct sockaddr_storage *ss, packet_t *pkt, size_t len){
}

void rel_recvpkt (rel_t *r, packet_t *pkt, size_t n){
  if (n < 8) return;
  uint16_t len = ntohs(pkt->len);
  if (len > n || len > 512 || len < 8 || (len > 8 && len < 12)) return;
 
  uint16_t received_cksum = pkt->cksum;
  pkt->cksum = 0;
  if (cksum(pkt, len) != received_cksum) return;
  pkt->cksum = received_cksum;
 
  uint32_t ackno = ntohl(pkt->ackno);
  if (ackno > r->next_seqno) return;
 
  packet_node_t *curr = r->tx_buffer;
  packet_node_t *prev = NULL;
  while (curr) {
    if (ntohl(curr->pkt.seqno) < ackno) {
      packet_node_t *tmp = curr;
      if (prev) prev->next = curr->next;
      else r->tx_buffer = curr->next;
      curr = curr->next;
      free(tmp);
    } 
    else {
      prev = curr;
      curr = curr->next;
    }
  }
 
  if (ackno > r->unacked_base) {
    r->unacked_base = ackno;
    rel_read(r);
  }
 
  if (len >= 12) {
    uint32_t seqno = ntohl(pkt->seqno);
        
    if (seqno >= r->expected_seqno && seqno < r->expected_seqno + r->window) {
      int is_dup = 0;
      packet_node_t *chk = r->rx_buffer;
      while (chk) {
        if (ntohl(chk->pkt.seqno) == seqno) is_dup = 1;
        chk = chk->next;
      }
            
      if (!is_dup) {
        packet_node_t *node = xmalloc(sizeof(packet_node_t));
        memcpy(&node->pkt, pkt, len);
        node->next = r->rx_buffer;
        r->rx_buffer = node;
      }
    }
 
    /* rel_output acks if it made progress; otherwise ack here (dups, gaps) */
    uint32_t old = r->expected_seqno;
    rel_output(r);
    if (r->expected_seqno == old) send_ack(r);
  }
 
  check_destroy(r);
}


void rel_read (rel_t *r){
  if (r->eof_read) return;
 
  while (r->next_seqno < r->unacked_base + r->window) {
    packet_t pkt;
    int res = conn_input(r->c, pkt.data, 500);
        
    if (res == 0) break;
        
    uint16_t pkt_len = 12 + (res > 0 ? res : 0);
    pkt.len = htons(pkt_len);
    pkt.seqno = htonl(r->next_seqno);
    pkt.ackno = htonl(r->expected_seqno);
    pkt.cksum = 0;
    pkt.cksum = cksum(&pkt, pkt_len);
 
    packet_node_t *node = xmalloc(sizeof(packet_node_t));
    memcpy(&node->pkt, &pkt, pkt_len);
    node->timestamp_ms = get_time_ms();
    node->retries = 0;
    node->next = NULL;
 
    if (!r->tx_buffer) {
      r->tx_buffer = node;
    } 
    else {
      packet_node_t *curr = r->tx_buffer;
      while (curr->next) curr = curr->next;
      curr->next = node;
    }
 
    conn_sendpkt(r->c, &node->pkt, pkt_len);
    r->next_seqno++;
 
    if (res == -1) {
      r->eof_read = 1;
      break;
    }
  }
 
  check_destroy(r);
}

void rel_output (rel_t *r){
  uint32_t old = r->expected_seqno;
 
  while (1) {
    packet_node_t *curr = r->rx_buffer;
    packet_node_t *prev = NULL;
 
    while (curr && ntohl(curr->pkt.seqno) != r->expected_seqno) {
      prev = curr;
      curr = curr->next;
    }
 
    if (!curr) break;
 
    int payload_size = ntohs(curr->pkt.len) - 12;
        
    if (payload_size > 0) {
      if (conn_bufspace(r->c) < payload_size) break;
      conn_output(r->c, curr->pkt.data, payload_size);
    } 
    else {
      conn_output(r->c, NULL, 0);
      r->eof_received = 1;
    }
 
    if (prev) prev->next = curr->next;
    else r->rx_buffer = curr->next;
    free(curr);
        
    r->expected_seqno++;
  }
 
  if (r->expected_seqno != old) send_ack(r);
  check_destroy(r);

}

void rel_timer (){
  /* Retransmit any packets that need to be retransmitted */
  rel_t *r = rel_list;
  rel_t *nx;
  long long now = get_time_ms();
 
  while (r) {
    nx = r->next;
 
    if (r->closing) {
      if (now - r->close_ts >= 2LL * r->timeout) rel_destroy(r);
      r = nx;
      continue;
    }
 
    int dead = 0;
    for (packet_node_t *p = r->tx_buffer; p; p = p->next) {
      if (now - p->timestamp_ms < r->timeout) continue;
      if (++p->retries > 5) {
        fprintf(stderr, "giving up after retries\n");
        dead = 1;
        break;
      }
      p->timestamp_ms = now;
      conn_sendpkt(r->c, &p->pkt, ntohs(p->pkt.len));
    }
    if (dead) rel_destroy(r);
 
    r = nx;
  }
}
