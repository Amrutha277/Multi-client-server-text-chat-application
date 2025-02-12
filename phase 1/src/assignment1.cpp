/**
 * @assignment1
 * @author  Team Members <ubitname@buffalo.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details at
 * http://www.gnu.org/copyleft/gpl.html
 *
 * @section DESCRIPTION
 *
 * This contains the main function. Add further description here....
 */
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <strings.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sstream>
#include <vector>
#include <bits/stdc++.h>

#include "../include/global.h"
#include "../include/logger.h"

#define BACKLOG 5
#define STDIN 0
#define TRUE 1
#define CMD_SIZE 100
#define BUFFER_SIZE 256
#define MSG_SIZE 256

using namespace std;

/**
 * main function
 *
 * @param  argc Number of arguments
 * @param  argv The argument list
 * @return 0 EXIT_SUCCESS
 */

int server(int argc, char **argv);
int client(int argc, char **argv);
int connect_to_host(char *server_ip, char *server_port);
char* fetchIP();
void getIPandPort(string command, char *cmd, char *server_IP, char *server_PORT);
int sendData(int socketFd, char *dataToSend, char *hostName, char *client_IP, char *client_PORT);
int isPortValid(char *server_PORT);
void createNodesFromList(char *clientInfo);
void displayClientDetails(char *cmd);
void sortClientDetails();
char* clientDetailsFromNodes();
void strDynamicConcat(char **str, const char *str2);

struct nodeClient *headNode;

struct nodeClient{
	int portNo;
	char hostName[60];
	char ipAddress[30];
	struct nodeClient *prev;
	struct nodeClient *next;	
};

int main(int argc, char **argv)
{
	/*Init. Logger*/
	cse4589_init_log(argv[2]);

	/* Clear LOGFILE*/
    fclose(fopen(LOGFILE, "w"));

	/*Start Here*/
	if(argv[1][0] == 's')
	{
		printf("in main s");
		int s = server(argc, argv);
	}
	else if(argv[1][0] == 'c')
	{
		printf("in main c");
		int c = client(argc, argv);
	}
	else
	{
		printf("Wrong command");
	}
	return 0;
}

int connect_to_host(char *server_ip, char* server_port)
{
	int fdsocket;
	struct addrinfo hints, *res;

	/* Set up hints structure */	
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;

	/* Fill up address structures */	
	if (getaddrinfo(server_ip, server_port, &hints, &res) != 0)
		perror("getaddrinfo failed");

	/* Socket */
	fdsocket = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
	if(fdsocket < 0)
		perror("Failed to create socket");
	
	/* Connect */
	if(connect(fdsocket, res->ai_addr, res->ai_addrlen) < 0)
		perror("Connect failed");
	
	freeaddrinfo(res);

	return fdsocket;
}

// Reference: https://simpledevcode.wordpress.com/2016/06/16/client-server-chat-in-c-using-sockets/
int server(int argc, char **argv)
{
	int serverSocketFd, head_socket, selret, sock_index, fdaccept=0;
	socklen_t caddr_len;
	struct sockaddr_in serverAddr;
	fd_set master_list, watch_list;

	if((serverSocketFd = socket(AF_INET, SOCK_STREAM, 0)) < 0){
		perror("Socket failed");
		exit(-1);
	}
	
	bzero(&serverAddr, sizeof(serverAddr));
	serverAddr.sin_family = AF_INET;
	serverAddr.sin_addr.s_addr = htonl(INADDR_ANY);
	serverAddr.sin_port = htons(atoi(argv[2]));

	printf("Port: %d", htons(serverAddr.sin_port));
	if(bind(serverSocketFd, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0){
		perror("Bind failed");
		exit(0);
	}

	if(listen(serverSocketFd, BACKLOG) < 0){
		perror("Unable to listen");
		exit(0);
	}
	/* ---------------------------------------------------------------------------- */
	
	// Zero select FD sets 
	FD_ZERO(&master_list);
	FD_ZERO(&watch_list);
	
	// Register the listening socket
	FD_SET(serverSocketFd, &master_list);
	// Register STDIN 
	FD_SET(STDIN, &master_list);
	
	head_socket = serverSocketFd;
	
	while(TRUE){
		memcpy(&watch_list, &master_list, sizeof(master_list));
		
		//printf("\n[PA1-Server@CSE489/589]$ ");
		//fflush(stdout);
		
		/* select() system call. This will BLOCK */
		selret = select(head_socket + 1, &watch_list, NULL, NULL, NULL);
		if(selret < 0)
			perror("select failed.");
		
		/* Check if we have sockets/STDIN to process */
		if(selret > 0){
			/* Loop through socket descriptors to check which ones are ready */
			for(sock_index=0; sock_index<=head_socket; sock_index+=1){
				
				if(FD_ISSET(sock_index, &watch_list)){
					
					/* Check if new command on STDIN */
					if (sock_index == STDIN){
						char *cmd = (char*) malloc(sizeof(char)*CMD_SIZE);
						
						memset(cmd, '\0', CMD_SIZE);
						if(fgets(cmd, CMD_SIZE-1, stdin) == NULL) //Mind the newline character that will be written to cmd
							exit(-1);
						
						strtok(cmd, "\n");
						//Process PA1 commands here ...
						int port = htons(serverAddr.sin_port);
						if(strcmp(cmd,"PORT") == 0){
							if(isPortValid(argv[2]) == 1){
								cse4589_print_and_log("[%s:SUCCESS]\n", cmd);
								cse4589_print_and_log("PORT:%d\n", port);
								cse4589_print_and_log("[%s:END]\n", cmd);
							}
							else{
								cse4589_print_and_log("[%s:ERROR]\n", cmd);
								cse4589_print_and_log("[%s:END]\n", cmd);
							}
						}
						else if(strcmp(cmd, "AUTHOR") == 0){
							cse4589_print_and_log("[%s:SUCCESS]\n", cmd);
							cse4589_print_and_log("I, %s, have read and understood the course academic integrity policy.\n", "npaluru-aramired");
							cse4589_print_and_log("[%s:END]\n", cmd);
						}
						else if(strcmp(cmd, "IP") == 0){
							char * serverIP = fetchIP();
							struct sockaddr_in tempIP;
							if(inet_pton(AF_INET, serverIP, &(tempIP.sin_addr)) == 1){
								cse4589_print_and_log("[%s:SUCCESS]\n", cmd);
								cse4589_print_and_log("IP:%s\n", serverIP);
								cse4589_print_and_log("[%s:END]\n", cmd);
							}
							else{
								cse4589_print_and_log("[%s:ERROR]\n", cmd);
								cse4589_print_and_log("[%s:END]\n", cmd);
							}
						}
						else if(strcmp(cmd, "LIST") == 0){
							sortClientDetails();
							displayClientDetails(cmd);
						}
						else if(strcmp(cmd, "EXIT") == 0){
							if(close(serverSocketFd) < 0){
								perror("Cannot close");
								cse4589_print_and_log("[%s:ERROR]\n", cmd);
								cse4589_print_and_log("[%s:END]\n", cmd);
							}
							else{
								cse4589_print_and_log("[%s:SUCCESS]\n", cmd);
								cse4589_print_and_log("[%s:END]\n", cmd);
							}
							exit(0);
						}
						else{
							printf("Wrong command");
						}
						
						free(cmd);
					}
					/* Check if new client is requesting connection */
					else if(sock_index == serverSocketFd){
						caddr_len = sizeof(serverAddr);
						fdaccept = accept(serverSocketFd, (struct sockaddr *)&serverAddr, &caddr_len);
						if(fdaccept < 0)
							perror("Accept failed.");
						
						printf("\nRemote Host connected!\n");                        
						
						/* Add to watched socket list */
						FD_SET(fdaccept, &master_list);
						if(fdaccept > head_socket) head_socket = fdaccept;
					}
					/* Read from existing clients */
					else{
						/* Initialize buffer to receieve response */
						char *buffer = (char*) malloc(sizeof(char)*BUFFER_SIZE);
						memset(buffer, '\0', BUFFER_SIZE);
						
						if(recv(sock_index, buffer, BUFFER_SIZE, 0) <= 0){
							close(sock_index);
							printf("Remote Host terminated connection!\n");
							
							/* Remove from watched list */
							FD_CLR(sock_index, &master_list);
						}
						else {
							//Process incoming data from existing clients here ...
							fdaccept = sock_index;
							char *clientList = (char*) malloc(sizeof(char)*BUFFER_SIZE);
							char *refreshList = (char*) malloc(sizeof(char)*BUFFER_SIZE);
							
							memset(clientList, '\0', BUFFER_SIZE);
							memset(refreshList, '\0', BUFFER_SIZE);

							printf("\nClient sent me: %s\n", buffer);
							printf("ECHOing it back to the remote host ... ");
							if(strncmp(buffer, "LOGIN", 5) == 0){
								createNodesFromList(buffer);
								clientList = clientDetailsFromNodes();
								printf("%s", clientList);
								if(send(fdaccept, clientList, strlen(clientList), 0) == strlen(clientList))
									printf("Done!\n");
								fflush(stdout);
								free(clientList);
							}
							else if(strcmp(buffer, "refresh") == 0){
								refreshList = clientDetailsFromNodes();
								printf("%s", refreshList);
								size_t refreshListLen = strlen(refreshList);
								if(send(fdaccept, refreshList, refreshListLen, 0) == strlen(refreshList))
									printf("Done!\n");
								fflush(stdout);
								free(refreshList);
							}
							else{
								printf("Not found");
							}
						}
						free(buffer);
					}
				}
			}
		}
	}
	return 0;
}
int client(int argc, char **argv)
{
	// if(argc != 3) {
	// 	printf("Usage:%s [ip] [port]\n", argv[0]);
	// 	exit(-1);
	// }
	
	int socketFd;
	struct sockaddr_in clientAddr;
	bool isLoggedIn;

	if((socketFd = socket(AF_INET, SOCK_STREAM, 0)) < 0){
		perror("Socket creation failed");
		exit(-1);
	}
	
	char * client_IP = fetchIP();
	bzero(&clientAddr, sizeof(clientAddr));
	clientAddr.sin_family = AF_INET;
	clientAddr.sin_addr.s_addr = inet_addr(client_IP);
	clientAddr.sin_port = htons(atoi(argv[2]));

	if(bind(socketFd, (struct sockaddr*)&clientAddr, sizeof(clientAddr)) < 0){
		perror("Bind failed");
		exit(0);
	}
	
	while(TRUE){
		fflush(stdout);
		
		char *command = (char*) malloc(sizeof(char)*MSG_SIZE);
		memset(command, '\0', MSG_SIZE);
		if(fgets(command, MSG_SIZE-1, stdin) == NULL) //Mind the newline character that will be written to command
			exit(-1);
		
		strtok(command, "\n");
		int port = htons(clientAddr.sin_port);
		if(strcmp(command,"PORT") == 0){
			if(port >=0 && port <= 65535){
				cse4589_print_and_log("[%s:SUCCESS]\n", command);
				cse4589_print_and_log("PORT:%d\n", port);
				cse4589_print_and_log("[%s:END]\n", command);
			}
			else{
				cse4589_print_and_log("[%s:ERROR]\n", command);
				cse4589_print_and_log("[%s:END]\n", command);
			}
		}
		else if(strcmp(command, "AUTHOR") == 0){
			cse4589_print_and_log("[%s:SUCCESS]\n", command);
			cse4589_print_and_log("I, %s, have read and understood the course academic integrity policy.\n", "npaluru-aramired");
			cse4589_print_and_log("[%s:END]\n", command);
		}
		else if(strcmp(command, "IP") == 0){
			char * serverIP = fetchIP();
			struct sockaddr_in tempIP;
			if(inet_pton(AF_INET, serverIP, &(tempIP.sin_addr)) == 1){
				cse4589_print_and_log("[%s:SUCCESS]\n", command);
				cse4589_print_and_log("IP:%s\n", serverIP);
				cse4589_print_and_log("[%s:END]\n", command);
			}
			else{
				cse4589_print_and_log("[%s:ERROR]\n", command);
				cse4589_print_and_log("[%s:END]\n", command);
			}
		}
		else if(strcmp(command, "EXIT") == 0){
			if(close(socketFd) < 0){
				perror("Cannot close");
				cse4589_print_and_log("[%s:ERROR]\n", command);
				cse4589_print_and_log("[%s:END]\n", command);
			}
			else{
				cse4589_print_and_log("[%s:SUCCESS]\n", command);
				cse4589_print_and_log("[%s:END]\n", command);
			}
			exit(0);
		}
		else if(strncmp(command, "LOGIN", 5) == 0){
			char cmd[8];
			char server_IP[20];
			char server_PORT[7];
			char hostName[60];
			//char client_IP[20];
			char * client_PORT = argv[2];
			getIPandPort(command, cmd, server_IP, server_PORT);

			//printf("%s", server_IP);
			//printf("%s", server_PORT);
			
			struct sockaddr_in tempIP;
			if(inet_pton(AF_INET, server_IP, &(tempIP.sin_addr)) == 1 && isPortValid(server_PORT) == 1){
				struct sockaddr_in remoteHostAddr;

				bzero(&remoteHostAddr, sizeof(remoteHostAddr));
				remoteHostAddr.sin_family = AF_INET;
				remoteHostAddr.sin_port = htons(atoi(server_PORT));
				inet_pton(AF_INET, server_IP, &remoteHostAddr.sin_addr);
				
				if(connect(socketFd, (struct sockaddr*)&remoteHostAddr, sizeof(remoteHostAddr)) < 0){
					perror("Server connect failed");
					exit(0);
				}
				else{
					char dataToSend[BUFFER_SIZE] = "LOGIN ";
					isLoggedIn = TRUE;
					if(getnameinfo((struct sockaddr*)&clientAddr, sizeof(clientAddr), hostName, 60, NULL, 0, 0) != 0){
						perror("Hostname retrival failed");
					}
					printf("Host name:%s", hostName);
					if(sendData(socketFd, dataToSend, hostName, client_IP, client_PORT) == 1){
						cse4589_print_and_log("[%s:SUCCESS]\n", cmd);
						cse4589_print_and_log("[%s:END]\n", cmd);
					}
					char *buffer = (char*) malloc(sizeof(char)*BUFFER_SIZE);
					memset(buffer, '\0', BUFFER_SIZE);

					if(recv(socketFd, buffer, BUFFER_SIZE, 0) > 0){
						printf("Server responded: %s", buffer);
						createNodesFromList(buffer);
						fflush(stdout);
					}
					free(buffer);
				}
				memset(cmd, 0, 8);
				memset(hostName, 0, 60);
				memset(server_IP, 0, 20);
				memset(server_PORT, 0, 7);
			}
			else{
				cse4589_print_and_log("[%s:ERROR]\n", cmd);
				cse4589_print_and_log("[%s:END]\n", cmd);
			}
		}
		else if(strcmp(command, "LIST") == 0){
			sortClientDetails();
			displayClientDetails(command);
		}
		else if(strcmp(command, "REFRESH") == 0){	
			char refreshMsg[10] = "refresh";
			if(send(socketFd, refreshMsg, strlen(refreshMsg), 0) == strlen(refreshMsg)){
				printf("Done refresh");
			}

			printf("before recv");

			char *buffer = (char*) malloc(sizeof(char)*BUFFER_SIZE);
			memset(buffer, '\0', BUFFER_SIZE);
			
			if(recv(socketFd, buffer, BUFFER_SIZE, 0) > 0){
				printf("Server responded: %s", buffer);
				headNode = NULL;
				createNodesFromList(buffer);
				cse4589_print_and_log("[%s:SUCCESS]\n", command);
				cse4589_print_and_log("[%s:END]\n", command);
				//fflush(stdout);
			}
			else{
				printf("recv not working");
			}
			free(buffer);
			printf("after recv");
		}
		else{
			printf("Wrong command");
		}
	}
}
char* fetchIP(){
	int dummySocketFd;
    struct sockaddr_in googleAddr, hostAddr;

    if ((dummySocketFd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) 
        perror("Error creating socket while getting IP of server");
	bzero(&googleAddr, sizeof(googleAddr));
    googleAddr.sin_family = AF_INET;
    googleAddr.sin_port = htons(53); // DNS port
    inet_pton(AF_INET, "8.8.8.8", &googleAddr.sin_addr);
    if (connect(dummySocketFd, (struct sockaddr *)&googleAddr, sizeof(googleAddr)) < 0)
        perror("Error connecting socket while getting IP of server");
    socklen_t hostAddrLen = sizeof(hostAddr);
    if (getsockname(dummySocketFd, (struct sockaddr *)&hostAddr, &hostAddrLen) < 0)
        perror("Getsockname failed while getting IP of server");

	char *serverIp = inet_ntoa(hostAddr.sin_addr);
    close(dummySocketFd);

	return serverIp;
}

// Reference: https://www.scaler.com/topics/split-string-in-cpp/
void getIPandPort(string command, char *cmd, char *server_IP, char *server_PORT){
	vector<string> arguments;
	stringstream element(command);
	string arg;

	while(element >> arg){
		arguments.push_back(arg);
	}
	if(arguments.size() >= 2){
		strcpy(cmd, arguments[0].c_str());
		strcpy(server_IP, arguments[1].c_str());
		strcpy(server_PORT, arguments[2].c_str());
	}
}
int sendData(int socketFd, char *dataToSend, char *hostName, char *client_IP, char *client_PORT){
	strcat(dataToSend, hostName);
	strcat(dataToSend, " ");
	strcat(dataToSend, client_IP);
	strcat(dataToSend, " ");
	strcat(dataToSend, client_PORT);
	int bytesSent = 0, len = strlen(dataToSend);
	int sentBytes = send(socketFd, dataToSend, len, 0);
	return sentBytes == len ? 1 : -1;
}
int isPortValid(char *server_PORT){
	int len = strlen(server_PORT);
	int isValid = -1, isDigit = 1;
	for(int i=0; i<len; i++){
		if(isdigit(server_PORT[i]) == 0){
			printf("Not a valid port as it contains non-numeric characters");
			isDigit = -1;
		}
	}
	if(atoi(server_PORT) >= 0 && atoi(server_PORT) <= 65535)
		isValid = 1;
	
	return (isDigit == 1 && isValid == 1) ? 1 : -1;
}
void createNodesFromList(char *clientInfo){
	vector<string> arguments;
	stringstream element(clientInfo);
	string arg, cmd;
	element >> arg;

	while(element >> arg){
		arguments.push_back(arg);
		element >> arg;
		arguments.push_back(arg);
		element >> arg;
		arguments.push_back(arg);

		struct nodeClient *dataNode = (struct nodeClient*) malloc(sizeof(struct nodeClient));
		strcpy(dataNode->hostName, arguments[0].c_str());
		strcpy(dataNode->ipAddress, arguments[1].c_str());
		dataNode->portNo = atoi(arguments[2].c_str());
		
		dataNode->next = NULL;
		if(headNode == NULL)
			headNode = dataNode;
		else{
			struct nodeClient *temp = headNode;

			while(temp->next != NULL){
				temp = temp->next;
			}
			temp->next = dataNode;
		}
		arguments.clear();
	}
}
void displayClientDetails(char *cmd){
	cse4589_print_and_log("[%s:SUCCESS]\n", cmd);

	struct nodeClient *temp = headNode;
	int n = 1;
	while(temp != NULL){
		cse4589_print_and_log("%-5d%-35s%-20s%-8d\n", n, temp->hostName, temp->ipAddress, temp->portNo);
		temp = temp->next;
		n++;
	}

	cse4589_print_and_log("[%s:END]\n", cmd);
}

//Reference: https://www.geeksforgeeks.org/bubble-sort-on-doubly-linked-list/
void sortClientDetails(){
	int swapped, i; 
    struct nodeClient *temp; 
    struct nodeClient *ptr = NULL; 
   
    if (headNode == NULL) 
        return; 
    do
    { 
        swapped = 0; 
        temp = headNode; 
        while (temp->next != ptr) 
        { 
            if (temp->portNo > temp->next->portNo) 
            {  
                swap(temp->portNo, temp->next->portNo);
				swap(temp->ipAddress, temp->next->ipAddress);
				swap(temp->hostName, temp->next->hostName); 
                swapped = 1; 
            } 
            temp = temp->next; 
        } 
        ptr = temp; 
    } while (swapped); 
}
char* clientDetailsFromNodes(){
	char* listOfClients = NULL; 
	char port[8];
	strDynamicConcat(&listOfClients, "clientList ");
	struct nodeClient *temp = headNode;
	while(temp != NULL){
		strDynamicConcat(&listOfClients, temp->hostName);
		strDynamicConcat(&listOfClients, " ");
		strDynamicConcat(&listOfClients, temp->ipAddress);
		strDynamicConcat(&listOfClients, " ");
		sprintf(port, "%d", temp->portNo);
		strDynamicConcat(&listOfClients, port);
		strDynamicConcat(&listOfClients, " ");
		temp = temp->next;
	}
	//printf("%s", listOfClients);
	return listOfClients;
}

// Reference: https://albertech.blogspot.com/2011/11/dynamically-concatenate-string-in-c-c99.html
void strDynamicConcat(char **str, const char *str2) {
    char *tmp = NULL;

    // Reset *str
    if ( *str != NULL && str2 == NULL ) {
        free(*str);
        *str = NULL;
        return;
    }

    // Initial copy
    if (*str == NULL) {
        *str = (char *)calloc( strlen(str2)+1, sizeof(char) );
        memcpy( *str, str2, strlen(str2) );
    }
    else { // Append
        tmp = (char *)calloc( strlen(*str)+1, sizeof(char) );
        memcpy( tmp, *str, strlen(*str) );
        *str = (char *)calloc( strlen(*str)+strlen(str2)+1, sizeof(char) );
        memcpy( *str, tmp, strlen(tmp) );
        memcpy( *str + strlen(*str), str2, strlen(str2) );
        free(tmp);
    }
} 