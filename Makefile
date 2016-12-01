CC = g++
CFLAGS = -std=gnu++11 -g -Wall

all: cs426_graph_server

cs426_graph_server: cs426_graph_server.c mongoose.c Graph.cpp checkpoint.c
	$(CC) $(CFLAGS) -o $@ $^

test: cs426_graph_server
	./cs426_graph_server

clean:
	$(RM) my_app *.o