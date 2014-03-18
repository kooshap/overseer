#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <time.h> 
#include "overseer.h"

int run_server()
{
	int listenfd = 0, connfd = 0, n=0;
	struct sockaddr_in serv_addr; 

	char sendBuff[1025], recvBuff[1024];
	time_t ticks; 

	listenfd = socket(AF_INET, SOCK_STREAM, 0);
	memset(&serv_addr, '0', sizeof(serv_addr));
	memset(sendBuff, '0', sizeof(sendBuff)); 

	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	serv_addr.sin_port = htons(5000); 

	bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)); 

	listen(listenfd, 10); 

	connfd = accept(listenfd, (struct sockaddr*)NULL, NULL); 

	while(1)
	{
		n = recv(connfd, recvBuff, sizeof(recvBuff)-1,0);
		if (n>0) {
			printf("%s\n",recvBuff);

			size_t key;
			int result;
			//sscanf(recvBuff,"%zd",&key);	
			char *tok = strtok(recvBuff, " ");
			char command=tok[0];
			tok = strtok(NULL, " ");
			sscanf(tok,"%zd",&key);	
			printf("KEY: %zd\n", key);
			
			if (command) {
				printf("OPERATION: %c\n", command);
				if (command=='r') {
					result=overseer_read(key);
					printf("Read:%d\n",result);
				}
				else if(command=='w') {
					overseer_write(key,"");
				}
				else if(command=='d') {
					overseer_delete(key);
				}
			}

			memset(recvBuff,0,strlen(recvBuff));

			snprintf(sendBuff, sizeof(sendBuff), "%d\n", result);
			send(connfd, sendBuff, strlen(sendBuff),0);
		}
		//close(connfd);
		//sleep(1);
	}

	return 0;
}
