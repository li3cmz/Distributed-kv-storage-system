from threading import Timer



# def hello():
#     print("hello")

# def timer():
#     hello()
#     Timer(5.58, timer).start()

def main():
    # Timer(5, timer).start()
    peers = {'node_1': ('localhost', 10001), 'node_2':('localhost', 10002)}
    for id in peers:
        print(peers[id][0] + ':' + str(peers[id][1]))

if __name__ == '__main__':
    main()