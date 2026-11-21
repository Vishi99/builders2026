#include <stdio.h>
#include <unistd.h>

int main(void) {
    const char* pipeline = __builtin_cpu_supports("x86-64-v4")
        ? "/app/shopping-xeon"
        : "/app/shopping-avx2";
    execl(pipeline, pipeline, (char*)0);
    perror(pipeline);
    return 127;
}
