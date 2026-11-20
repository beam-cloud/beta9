// Turns TCP keepalive on for every connection this machine opens: a probe
// once a connection has been quiet for KEEPALIVE_SECONDS, and at that interval
// from then on. Run once, as root, before anything that should be covered.
//
// A program keeps the connections it has finished with, to use them again.
// Something on the way may have forgotten such a connection by then, and it
// tells neither end: the next request goes unanswered until the program gives
// up on it. The runner uploads a job's logs to Azure Storage on a connection
// it keeps for a minute, and gives an upload 30 seconds. From some networks
// Azure's end forgets a connection that has been quiet for 16 seconds, so a
// job that went longer than that without an upload ended half a minute after
// its last step. A probe every 5 seconds leaves no connection quiet for that
// long, with two probes to lose.
//
// Linux has no setting that does this for programs that do not ask, so the
// kernel is given a BPF program to run on each connection as it is
// established. It is attached to the root cgroup, which every process of the
// machine is under, containers too:
//
//     if (ctx->op != BPF_SOCK_OPS_ACTIVE_ESTABLISHED_CB)
//         return 1;
//     int value = KEEPALIVE_SECONDS;
//     bpf_setsockopt(ctx, IPPROTO_TCP, TCP_KEEPIDLE, &value, sizeof(value));
//     bpf_setsockopt(ctx, IPPROTO_TCP, TCP_KEEPINTVL, &value, sizeof(value));
//     value = 1;
//     bpf_setsockopt(ctx, SOL_SOCKET, SO_KEEPALIVE, &value, sizeof(value));
//     return 1;
//
// A program that sets these itself once it has connected still has its way.
// How many probes may go unanswered before the kernel closes the connection
// is left at its default of nine.
#include <fcntl.h>
#include <linux/bpf.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

#define KEEPALIVE_SECONDS 5

#define INSN(CODE, DST, SRC, OFF, IMM) \
    {.code = (CODE), .dst_reg = (DST), .src_reg = (SRC), .off = (OFF), .imm = (IMM)}
#define MOV_REG(DST, SRC) INSN(BPF_ALU64 | BPF_MOV | BPF_X, DST, SRC, 0, 0)
#define MOV_IMM(DST, IMM) INSN(BPF_ALU64 | BPF_MOV | BPF_K, DST, 0, 0, IMM)
#define ADD_IMM(DST, IMM) INSN(BPF_ALU64 | BPF_ADD | BPF_K, DST, 0, 0, IMM)
#define LOAD_U32(DST, SRC, OFF) INSN(BPF_LDX | BPF_MEM | BPF_W, DST, SRC, OFF, 0)
#define STORE_U32(DST, OFF, IMM) INSN(BPF_ST | BPF_MEM | BPF_W, DST, 0, OFF, IMM)
#define JUMP_IF_NOT(DST, IMM, OFF) INSN(BPF_JMP | BPF_JNE | BPF_K, DST, 0, OFF, IMM)
#define CALL(HELPER) INSN(BPF_JMP | BPF_CALL, 0, 0, 0, HELPER)
#define EXIT() INSN(BPF_JMP | BPF_EXIT, 0, 0, 0, 0)

// The value is the four bytes at the top of the stack, and r6 holds ctx.
#define VALUE (-4)
#define SET_VALUE(IMM) STORE_U32(BPF_REG_10, VALUE, IMM)
#define SETSOCKOPT_INSNS 7
#define SETSOCKOPT(LEVEL, NAME)       \
    MOV_REG(BPF_REG_1, BPF_REG_6),    \
    MOV_IMM(BPF_REG_2, LEVEL),        \
    MOV_IMM(BPF_REG_3, NAME),         \
    MOV_REG(BPF_REG_4, BPF_REG_10),   \
    ADD_IMM(BPF_REG_4, VALUE),        \
    MOV_IMM(BPF_REG_5, sizeof(int)),  \
    CALL(BPF_FUNC_setsockopt)

static const struct bpf_insn program[] = {
    MOV_REG(BPF_REG_6, BPF_REG_1),
    LOAD_U32(BPF_REG_2, BPF_REG_6, offsetof(struct bpf_sock_ops, op)),
    // Over everything up to the return.
    JUMP_IF_NOT(BPF_REG_2, BPF_SOCK_OPS_ACTIVE_ESTABLISHED_CB, 2 + 3 * SETSOCKOPT_INSNS),
    SET_VALUE(KEEPALIVE_SECONDS),
    SETSOCKOPT(IPPROTO_TCP, TCP_KEEPIDLE),
    SETSOCKOPT(IPPROTO_TCP, TCP_KEEPINTVL),
    SET_VALUE(1),
    SETSOCKOPT(SOL_SOCKET, SO_KEEPALIVE),
    MOV_IMM(BPF_REG_0, 1),
    EXIT(),
};

static long bpf(int command, union bpf_attr *attr) {
    return syscall(SYS_bpf, command, attr, sizeof(*attr));
}

static int load(char *log, size_t size) {
    union bpf_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.prog_type = BPF_PROG_TYPE_SOCK_OPS;
    attr.insns = (unsigned long)program;
    attr.insn_cnt = sizeof(program) / sizeof(program[0]);
    // Of this repository. The program calls nothing that is only for the GPL.
    attr.license = (unsigned long)"AGPL-3.0";
    strcpy(attr.prog_name, "tcp_keepalive");
    if (log) {
        attr.log_buf = (unsigned long)log;
        attr.log_size = size;
        attr.log_level = 1;
    }
    return bpf(BPF_PROG_LOAD, &attr);
}

int main(void) {
    int program_fd = load(NULL, 0);
    if (program_fd < 0) {
        perror("tcp-keepalive: load the program");
        // Again, for the kernel's reason.
        static char log[1 << 16];
        load(log, sizeof(log));
        fputs(log, stderr);
        return 1;
    }

    int cgroup_fd = open("/sys/fs/cgroup", O_RDONLY | O_DIRECTORY);
    if (cgroup_fd < 0) {
        perror("tcp-keepalive: open /sys/fs/cgroup");
        return 1;
    }
    union bpf_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.target_fd = cgroup_fd;
    attr.attach_bpf_fd = program_fd;
    attr.attach_type = BPF_CGROUP_SOCK_OPS;
    // A job may attach programs of its own.
    attr.attach_flags = BPF_F_ALLOW_MULTI;
    if (bpf(BPF_PROG_ATTACH, &attr) < 0) {
        perror("tcp-keepalive: attach the program to /sys/fs/cgroup");
        return 1;
    }
    return 0;
}
