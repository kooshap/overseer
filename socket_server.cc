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
#include <event2/event.h>
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
			//printf("%s\n",recvBuff);

			size_t key;
			char command,*tok,*result,*value;
			//sscanf(recvBuff,"%zd",&key);	
			tok = strtok(recvBuff, " ");
			if (tok!=NULL) {
				command=tok[0];
				tok = strtok(NULL, " ");
			}
			if (tok!=NULL) {
				sscanf(tok,"%zd",&key);	
				printf("KEY: %zd\n", key);
				tok = strtok(NULL, " ");
			} 
			if (tok!=NULL) {
				int vlen=strlen(tok);
				value=(char *)malloc(vlen*sizeof(*value));
				strncpy(value,tok,vlen);
				if (value[vlen-1]=='\n') {
					value[vlen-1]='\0';
				}
				printf("VALUE:%s\n",value);
			}

			if (command) {
				printf("OPERATION: %c\n", command);
				if (command=='r') {
					result=overseer_read(key);
					printf("Read:%s\n",result);
					snprintf(sendBuff, sizeof(sendBuff), "%s\n", result);
				}
				else if(command=='w') {
					overseer_write(key,value);
					snprintf(sendBuff, sizeof(sendBuff), "1\n");
				}
				else if(command=='d') {
					overseer_delete(key);
					snprintf(sendBuff, sizeof(sendBuff), "1\n");
				}
			}
			memset(recvBuff,0,strlen(recvBuff));
			send(connfd, sendBuff, strlen(sendBuff),0);
			memset(recvBuff,0,strlen(recvBuff));
		}
		//close(connfd);
		//sleep(1);
	}

	return 0;
}
