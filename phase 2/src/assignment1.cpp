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
#include <iostream>
#include <string>
#include <list>

#include "../include/global.h"
#include "../include/logger.h"

#define BACKLOG 5
#define STDIN 0
#define TRUE 1
#define CMD_SIZE 512
#define BUFFER_SIZE 512
#define MSG_SIZE 512

using namespace std;

/**
 * main function
 *
 * @param  argc Number of arguments
 * @param  argv The argument list
 * @return 0 EXIT_SUCCESS
 */

// https://www.codecademy.com/resources/docs/cpp/strings/strtok

int server(int argc, char **argv);
int client(int argc, char **argv);
int connect_to_host(char *server_ip, char *server_port);
char* fetchIP();
void getIPandPort(string command, char *cmd, char *server_IP, char *server_PORT);
int sendData(int socketFd, char *dataToSend, char *hostName, char *client_IP, char *client_PORT);
int isPortValid(char *server_PORT);
void createClient(char *clientInfo, int socketFd);
void createNodesFromList(char *clientInfo);
void displayClientDetails(char *cmd);
void serverDisplayClientDetails(char *cmd);
void displayStatsOfClients(char *cmd);
void displayBLockedClientDetails(char *cmd);
void sortClientDetails();
char* clientDetailsFromNodes();
void strDynamicConcat(char **str, const char *str2);
int isIPLoggedIn(char* ip);
int isIPBlocked(char* fromIP, char* toIP);
void sendOrBuffer(char* fromIP, char* toIP, char* msg);
void broadcastOrBuffer(char* fromIP, char* toIP, char* msg);
//std::map<std::string, std::array<std::string, 10>> blockMap;
//string blockPairs[5][5]; 

void updateLoggedIn(char* ip);
struct nodeClient *headNode;
//void addBlockedIP(nodeClient* headNode, const char* blocking_ip);

struct nodeClient{
	int socketFd = 0;
	int portNo;
	//int isBlocked;
	//string blockList = " ";
	char hostName[60];
	char ipAddress[30];
	int msgSent = 0;
	int msgReceived = 0;
	int loginStatus = 0;
	vector<string> bufferedMsgs;
	vector<char*> blockedIPs;
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
		
		// printf("\n$ ");
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
						nodeClient* temp = headNode;

						while(temp!=NULL){
							cout<<temp->ipAddress<<endl;
							// vector<string> vec = temp->blockedIPs;
								for (const char* message : temp->blockedIPs) {
								std::cout<<"before input messages: " << message << std::endl;
							}
							temp = temp->next;
						}
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

							nodeClient* temp = headNode;
							while(temp!=NULL){
								cout<<temp->ipAddress<<endl;
									for (const char* message : temp->blockedIPs) {
									std::cout<<"clientDetailsFromNodes messages after: " << message << std::endl;
								}
								temp = temp->next;
							}
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
							serverDisplayClientDetails(cmd);
						}
						 else if(strncmp(cmd, "BLOCKED",7)== 0){
						 	// strncmp(buffer, "BLOCK", 5
                            displayBLockedClientDetails(cmd);

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
						else if(strcmp(cmd, "STATISTICS") == 0){
							sortClientDetails();
							displayStatsOfClients(cmd);
						}
						else if(strcmp(cmd, "DETAILS") == 0){
							struct nodeClient *temp = headNode;
							int n = 1;
							while(temp != NULL){
								// cse4589_print_and_log("%-5d%-35s%-20s%-8d%-8d%-8d\n", n, temp->hostName, temp->ipAddress, temp->portNo, temp->socketFd, temp->loginStatus);
								temp = temp->next;
								n++;
							}
						}
						else{
							printf("Wrong command");
						}
						// free(cmd);
					}
					/* Check if new client is requesting connection */
					else if(sock_index == serverSocketFd){
						caddr_len = sizeof(serverAddr);
						cout<<"in new connection"<<endl;
						fdaccept = accept(serverSocketFd, (struct sockaddr *)&serverAddr, &caddr_len);
						if(fdaccept < 0)
							perror("Accept failed.");
						
						printf("\nRemote Host connected!\n"); 
						// nodeClient* temp = headNode;                      
						// while(temp!=NULL){
						// 	cout<<temp->ipAddress<<endl;
						// 		// vector<string> vec = temp->blockedIPs;
						// 	for (const char* message : temp->blockedIPs) {
						// 	std::cout<<"login messages: " << message << std::endl;
						// }
						// 	temp = temp->next;
						// }
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
								printf("FD: %d", fdaccept);
								cout<<"buffer mssg: "<<buffer<<endl;
								createClient(buffer, fdaccept);
								clientList = clientDetailsFromNodes();
								printf("%s", clientList);
								if(send(fdaccept, clientList, strlen(clientList), 0) == strlen(clientList))
									printf("Done!\n");
								fflush(stdout);
								// free(clientList);
							}
							else if(strcmp(buffer, "refresh") == 0){
								refreshList = clientDetailsFromNodes();
								printf("%s", refreshList);
								size_t refreshListLen = strlen(refreshList);
								if(send(fdaccept, refreshList, refreshListLen, 0) == strlen(refreshList))
									printf("Done!\n");
								fflush(stdout);
								nodeClient* temp = headNode;
								while(temp!=NULL){
									cout<<temp->ipAddress<<endl;
									// vector<string> vec = temp->blockedIPs;
									 for (const char* message : temp->blockedIPs) {
        								std::cout<<"refresh messages: " << message << std::endl;
    								}
									temp = temp->next;
								}
								// free(refreshList);
								while(temp!=NULL){
									cout<<temp->ipAddress<<endl;
									// vector<string> vec = temp->blockedIPs;
									 for (const char* message : temp->blockedIPs) {
        								std::cout<<"refresh messages after free: " << message << std::endl;
    								}
									temp = temp->next;
								}
							}
							else if(strncmp(buffer, "SEND", 4) == 0){
								cout<<"In send: "<<endl;

								char* tokens = strtok(buffer, " ");
								char * cmd = tokens;
								tokens = strtok(NULL, " ");
								char * fromIP = tokens;
								tokens = strtok(NULL, " ");
								char * toIP = tokens;
								tokens = strtok(NULL, "");
								char * msg = tokens;
								//MSSG SEND 
								string mssg_str(msg);
								size_t pos = mssg_str.find("SEND");
								if (pos != std::string::npos) {
									string mssg1 = mssg_str.substr(0,pos);
									cout<<"mssg1: "<<mssg_str<<endl;
									char* mssg1_ptr = new char[mssg1.size() + 1];
    								std::strcpy(mssg1_ptr, mssg1.c_str());


									if(isIPBlocked(fromIP, toIP) == 0){
										sendOrBuffer(fromIP, toIP, mssg1_ptr);
									}
									string mssg2_total = mssg_str.substr(pos+5);
									
									cout<<"mssg2_total: "<<mssg2_total<<endl;
									size_t pos2 = mssg2_total.find(" ");
									string another_ip = mssg2_total.substr(0,pos2);
									cout<<"another ip: "<<another_ip<<endl;
									char* another_ip_ptr = new char[another_ip.size() + 1];
    								std::strcpy(another_ip_ptr, another_ip.c_str());

									string final_mssg = mssg2_total.substr(pos2+1);
									char* final_mssg_ptr = new char[final_mssg.size() + 1];
    								std::strcpy(final_mssg_ptr, final_mssg.c_str());
									if(isIPBlocked(fromIP,another_ip_ptr) == 0){
										sendOrBuffer(fromIP,another_ip_ptr,final_mssg_ptr);
									}
								}else{
									if(isIPBlocked(fromIP, toIP) == 0){
										sendOrBuffer(fromIP, toIP, msg);
									}
								}
							
							}
							else if(strncmp(buffer, "BROADCAST", 9) == 0){
								char* tokens = strtok(buffer, " ");
								char * cmd = tokens;
								tokens = strtok(NULL, " ");
								char * fromIP = tokens;
								char * toIP = (char*) malloc(sizeof(char)*20);
								strcpy(toIP, "255.255.255.255");
								tokens = strtok(NULL, "");
								char * msg = tokens;

								string mssg_str(msg);
								size_t pos = mssg_str.find("BROADCAST");
								if (pos != std::string::npos) {
									string mssg1 = mssg_str.substr(0,pos);
									cout<<"mssg1: "<<mssg_str<<endl;
									char* mssg1_ptr = new char[mssg1.size() + 1];
    								std::strcpy(mssg1_ptr, mssg1.c_str());

									broadcastOrBuffer(fromIP, toIP, mssg1_ptr);
									
									string mssg2_total = mssg_str.substr(pos+9);
									
									cout<<"mssg2_total: "<<mssg2_total<<endl;
									// size_t pos2 = mssg2_total.find(" ");

									// string final_mssg = mssg2_total.substr(pos2+1);
									char* final_mssg_ptr = new char[mssg2_total.size() + 1];
    								std::strcpy(final_mssg_ptr, mssg2_total.c_str());
									broadcastOrBuffer(fromIP,toIP,final_mssg_ptr);
								}
								else{
									broadcastOrBuffer(fromIP, toIP, msg);
								}
								
							}
							else if (strncmp(buffer, "BLOCK", 5) == 0) {
								cout<<"BLOCK in server: "<<buffer<<endl;
								char* tokens = strtok(buffer, " ");
								char * cmd = tokens;
								tokens = strtok(NULL, " ");
								char * fromIP = tokens;
								tokens = strtok(NULL, " ");
								char * toIP = tokens;
								nodeClient* temp = headNode;
								while(temp != NULL){
									if(strcmp(temp->ipAddress, fromIP) == 0){
										cout<<"TO IP: "<<toIP<<endl;
										temp->blockedIPs.push_back(toIP);
									}
									temp = temp->next;
								}
								temp = headNode;

								while(temp!=NULL){
									cout<<temp->ipAddress<<endl;
									// vector<string> vec = temp->blockedIPs;
									 for (const char* message : temp->blockedIPs) {
        								std::cout<<"blck messages: " << message << std::endl;
    								}
									temp = temp->next;
								}
							}								
							else if (strncmp(buffer, "UNBLOCK", 7) == 0) {
								cout<<"UNBLOCK in server: "<<buffer<<endl;
								char* tokens = strtok(buffer, " ");
								char * cmd = tokens;
								tokens = strtok(NULL, " ");
								char * fromIP = tokens;
								tokens = strtok(NULL, " ");
								char * toIP = tokens;
								nodeClient* temp = headNode;
								// temp = headNode;
								while(temp!=NULL){
									cout<<temp->ipAddress<<endl;
									// vector<string> vec = temp->blockedIPs;
									 for (const char* message : temp->blockedIPs) {
        								std::cout<<"blck messages before erase: " << message << std::endl;
    								}
									temp = temp->next;
								}
								temp = headNode;
								while(temp != NULL){
									if(strcmp(temp->ipAddress, fromIP) == 0){
										printf("in unblock erase");
										cout << "Comparing: " << temp->ipAddress << " and " << fromIP << endl;
										 if (!temp->blockedIPs.empty()) {
										if (temp->blockedIPs.size() == 1) {
											// Erase the single element in blockedIPs
											temp->blockedIPs.erase(temp->blockedIPs.begin());
											cout << "Removed single IP from blockedIPs" << endl;
										} else {
											// Find the IP and erase it
											auto it = std::find(temp->blockedIPs.begin(), temp->blockedIPs.end(), toIP);
											if (it != temp->blockedIPs.end()) {
												temp->blockedIPs.erase(it);
												cout << "Removed " << toIP << " from blockedIPs" << endl;
											} else {
												cout << toIP << " not found in blockedIPs" << endl;
											}
										}
									} else {
										cout << "blockedIPs is empty" << endl;
									}
									break;
									}
									temp = temp->next;
								}
								temp = headNode;
								while(temp!=NULL){
									cout<<temp->ipAddress<<endl;
									// vector<string> vec = temp->blockedIPs;
									 for (const char* message : temp->blockedIPs) {
        								std::cout<<"blck messages after erase: " << message << std::endl;
    								}
									temp = temp->next;
								}
								
								
							}
							else if(strncmp(buffer, "LOGOUT", 6)== 0){
								char *token = strtok(buffer, " "); 
								token = strtok(NULL, " ");
								char * client_IP_to_logout = token;
								nodeClient* temp = headNode;

								while(temp != NULL){
									if (strcmp(temp->ipAddress, client_IP_to_logout) == 0){
										temp->loginStatus = 0;
									}
									temp = temp->next;
								}
								//printf("logged out client: ", client_IP_to_logout);
							}
							else{
								//printf("Not found");
							}
						}
						// free(buffer);
					}

					// nodeClient* temp = headNode;

					// 	while(temp!=NULL){
					// 		cout<<temp->ipAddress<<endl;
					// 		// vector<string> vec = temp->blockedIPs;
					// 			for (const char* message : temp->blockedIPs) {
					// 			std::cout<<"after saving to block messages: " << message << std::endl;
					// 		}
					// 		temp = temp->next;
					// 	}
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
	
	int socketFd, head_socket, selret, sock_index, fdaccept=0;
    socklen_t caddr_len;
	struct sockaddr_in clientAddr;
	bool isNotExited = false;
	bool isLoggedIn = false;
    fd_set master_list, watch_list;
	vector<char*> blockedIpLocal;

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

    // Zero select FD sets 
	FD_ZERO(&master_list);
	FD_ZERO(&watch_list);
	
	// Register the listening socket
	FD_SET(socketFd, &master_list);
	// Register STDIN 
	FD_SET(STDIN, &master_list);
	
	head_socket = socketFd;
	
	while(TRUE){
		memcpy(&watch_list, &master_list, sizeof(master_list));
    
        // printf("\n$ ");
        //fflush(stdout);
        
        /* select() system call. This will BLOCK */
        selret = select(head_socket + 1, &watch_list, NULL, NULL, NULL);
        if(selret < 0)
            perror("select failed.");
        for (const char* ip : blockedIpLocal) {
			std::cout << "before loop: " << ip << std::endl;
		}
        /* Check if we have sockets/STDIN to process */
        if(selret > 0){
            /* Loop through socket descriptors to check which ones are ready */
            for(sock_index=0; sock_index<=head_socket; sock_index+=1){
                
                if(FD_ISSET(sock_index, &watch_list)){
                    
                    /* Check if new command on STDIN */
                    if (sock_index == STDIN){
		
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
// <<<<<<< HEAD
                        // else if(strcmp(command, "LOGOUT") == 0) {
							//printf("in logout %s/n");
// =======
                        else if((strcmp(command, "LOGOUT") == 0) && isLoggedIn){
							char * dataToSend = (char*) malloc(sizeof(char)*MSG_SIZE);
							strcpy(dataToSend, command);
							strcat(dataToSend, " ");
							strcat(dataToSend, client_IP);
							printf("\ndata: %s", dataToSend);

							//if ((send(socketFd, dataToSend, strlen(dataToSend), 0) == strlen (dataToSend)) && isLoggedIn) {
								//printf("Sending server to logout client with %s\n", command);
							if (isLoggedIn) {
								// send(socketFd, dataToSend, strlen(dataToSend), 0);
								// cse4589_print_and_log("[%s:SUCCESS]\n", command);
								// cse4589_print_and_log("[%s:END]\n", command);
// =======
							// printf("\ndata: %s", dataToSend);
							cse4589_print_and_log("[%s:SUCCESS]\n", "LOGOUT");
							cse4589_print_and_log("[%s:END]\n", "LOGOUT");
							if (send(socketFd, dataToSend, strlen(dataToSend), 0) == strlen (dataToSend)) {
								// printf("Sending server to logout client with %s\n", command);
// >>>>>>> 9eed210de3a1c616b4828afa17dd401199c8aa65
								isLoggedIn = false;
							} else {
// <<<<<<< HEAD
								//perror("Error sending logout command");								
								cse4589_print_and_log("[%s:ERROR]\n", command);
								cse4589_print_and_log("[%s:END]\n", command);
// =======
								perror("Error sending logout command");								
// >>>>>>> 9eed210de3a1c616b4828afa17dd401199c8aa65
							}
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
								isNotExited = false;
								isLoggedIn = false;
                            }
                            exit(0);
                        }
                        else if(strncmp(command, "LOGIN", 5) == 0){
                            // if(isIPLoggedIn == 0){

							// 	updateLoggedIn(client_IP);
							// 	cse4589_print_and_log("[%s:SUCCESS]\n", "LOGIN");
                            //     cse4589_print_and_log("[%s:END]\n", "LOGIN");
							// }
							if(isLoggedIn){
								printf("logged in again");
								// cse4589_print_and_log("[%s:SUCCESS]\n", "LOGIN");
								// cse4589_print_and_log("[%s:END]\n", "LOGIN");
							}
							else{
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
									if(!isNotExited){
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
											if(getnameinfo((struct sockaddr*)&clientAddr, sizeof(clientAddr), hostName, 60, NULL, 0, 0) != 0){
												perror("Hostname retrival failed");
											}
											printf("Host name:%s", hostName);
											isNotExited = true;
											isLoggedIn = true;
											if(sendData(socketFd, dataToSend, hostName, client_IP, client_PORT) == 1){
												printf("Send success");
												cse4589_print_and_log("[%s:SUCCESS]\n", cmd);
												cse4589_print_and_log("[%s:END]\n", cmd);
											}
											// char *buffer = (char*) malloc(sizeof(char)*BUFFER_SIZE);
											// memset(buffer, '\0', BUFFER_SIZE);

											// if(recv(socketFd, buffer, BUFFER_SIZE, 0) > 0){
											//     printf("Server responded: %s", buffer);
											//     createNodesFromList(buffer);
											//     fflush(stdout);
											// }
											// free(buffer);
										}
									}
									else{
										char dataToSend[BUFFER_SIZE] = "LOGIN ";
										if(getnameinfo((struct sockaddr*)&clientAddr, sizeof(clientAddr), hostName, 60, NULL, 0, 0) != 0){
											perror("Hostname retrival failed");
										}
										printf("Host name:%s", hostName);
										isNotExited = true;
										isLoggedIn = true;
										if(sendData(socketFd, dataToSend, hostName, client_IP, client_PORT) == 1){
											printf("Send success");
											cse4589_print_and_log("[%s:SUCCESS]\n", "LOGIN");
											cse4589_print_and_log("[%s:END]\n", "LOGIN");
										}
										// isNotExited = true;
										// isLoggedIn = true;
										// char dataToSend[BUFFER_SIZE] = "LAGAIN ";
										// strcat(dataToSend, client_IP);
										// nodeClient *temp = headNode;
										// while(temp != NULL){
										// 	if(strcmp(temp->ipAddress, client_IP) == 0){
										// 		temp->loginStatus = 1;
										// 	}
										// 	temp = temp->next;
										// }
										// if(send(socketFd, dataToSend, strlen(dataToSend), 0) == strlen(dataToSend)){
										// 	cse4589_print_and_log("[%s:SUCCESS]\n", cmd);
										// 	cse4589_print_and_log("[%s:END]\n", cmd);
										// }
										
									}
								}
								else{
									cse4589_print_and_log("[%s:ERROR]\n", cmd);
									cse4589_print_and_log("[%s:END]\n", cmd);
								}
							}
                        }
                        else if((strcmp(command, "LIST") == 0) && isLoggedIn){
                            sortClientDetails();
                            displayClientDetails(command);
                        }
                        else if((strcmp(command, "REFRESH") == 0) && isLoggedIn){	
                            char refreshMsg[10] = "refresh";
                            if(send(socketFd, refreshMsg, strlen(refreshMsg), 0) == strlen(refreshMsg)){
                                cse4589_print_and_log("[%s:SUCCESS]\n", command);
                                cse4589_print_and_log("[%s:END]\n", command);
                            }

                            // char *buffer = (char*) malloc(sizeof(char)*BUFFER_SIZE);
                            // memset(buffer, '\0', BUFFER_SIZE);
                            
                            // if(recv(socketFd, buffer, BUFFER_SIZE, 0) > 0){
                            //     printf("Server responded: %s", buffer);
                            //     headNode = NULL;
                            //     createNodesFromList(buffer);
                            //     //fflush(stdout);
                            // }
                            // else{
                            //     printf("recv not working");
                            // }
                            // free(buffer);
                            
                        }
						else if((strncmp(command, "BLOCK", 5) == 0) && isLoggedIn){
							char* tokens = strtok(command, " ");
							char * cmd = tokens;
							tokens = strtok(NULL, " ");
							char * toIp = tokens;
							struct sockaddr_in tempIP;
							char * sendMsg = (char*) malloc(sizeof(char)*MSG_SIZE);
							strcpy(sendMsg, cmd);
							strcat(sendMsg, " ");
							strcat(sendMsg, client_IP);
							strcat(sendMsg, " ");
							strcat(sendMsg, toIp);
							nodeClient* temp = headNode;
							bool isValid = false;
							bool isPresent = false;
							while(temp != NULL){
								if(strcmp(temp->ipAddress, toIp) == 0){
									isValid = true;
								}
								temp = temp->next;
							}
							cout<<"valid: "<<isValid<<endl;
							cout<<"toIP: "<<toIp<<endl;
							for(char* ip: blockedIpLocal){
								if(strcmp(ip, toIp) == 0){
									isPresent = true;
								}
							}
							
							cout<<"isPresent: "<<isPresent<<endl;
							if(isValid){
								if(!isPresent){
									cout<<"push This value: "<<toIp<<endl;
									blockedIpLocal.push_back(toIp);
									cout<<"blockedIpLocal[0]: "<< blockedIpLocal[0]<<endl;
								}
							}
							
							// for (const char* ip : blockedIpLocal) {
							// 	std::cout << "IP: " << ip << std::endl;
							// }
							// nodeClient* temp = headNode;
							// //bool isValid = false;
							// while(temp != NULL){
							// 	if(strcmp(temp->ipAddress, ip) == 0){
							// 		if(temp->loginStatus == 1){
							// 			isValid = true;
							// 		}
							// 	}
							// 	temp = temp->next;
							// }

							if(inet_pton(AF_INET, toIp, &(tempIP.sin_addr)) == 1 && isValid && !isPresent){
								if(send(socketFd, sendMsg, strlen(sendMsg), 0) == strlen(sendMsg)){
									cse4589_print_and_log("[%s:SUCCESS]\n", cmd);
									cse4589_print_and_log("[%s:END]\n", cmd);
                            	}
							}
							else{
								cse4589_print_and_log("[%s:ERROR]\n", cmd);
								cse4589_print_and_log("[%s:END]\n", cmd);
							}
						}
						else if((strncmp(command, "UNBLOCK", 7) == 0) && isLoggedIn){
							char* tokens = strtok(command, " ");
							char * cmd = tokens;
							tokens = strtok(NULL, " ");
							char * toIp = tokens;
							struct sockaddr_in tempIP;
							char * sendMsg = (char*) malloc(sizeof(char)*MSG_SIZE);
							strcpy(sendMsg, cmd);
							strcat(sendMsg, " ");
							strcat(sendMsg, client_IP);
							strcat(sendMsg, " ");
							strcat(sendMsg, toIp);
							nodeClient* temp = headNode;
							bool isValid = false;
							//bool isPresent = false;
							while(temp != NULL){
								if(strcmp(temp->ipAddress, toIp) == 0 && temp->portNo != port){
									isValid = true;
								}
								temp = temp->next;
							}
							// for(char* ip: blockedIpLocal){
							// 	if(strcmp(ip, toIp) == 0){
							// 		isPresent = 1;
							// 	}
							// }
							bool isPresent = true;
							cout<<"blockedIp size: "<<blockedIpLocal.size()<<endl;
							cout<<"toIP: "<<toIp<<endl;
							if (blockedIpLocal.size() == 1) {
								auto it = std::find(blockedIpLocal.begin(), blockedIpLocal.end(), toIp);
								for (const char* ip : blockedIpLocal) {
									std::cout << "IP: " << ip << std::endl;
									if(strcmp(ip,toIp)==0){
										blockedIpLocal.erase(blockedIpLocal.begin());
										std::cout << "Removed " << toIp << " from blockedIpLocal" << std::endl;
									}else{
										isPresent = false;
										std::cout << toIp << " not found in blockedIpLocal" << std::endl;
									}
								}
							}else{
								auto it = std::find(blockedIpLocal.begin(), blockedIpLocal.end(), toIp);
								if (it != blockedIpLocal.end()) {
									// Erase the element at the found position
									blockedIpLocal.erase(it);
									std::cout << "Removed " << toIp << " from blockedIpLocal" << std::endl;
								} else {
									isPresent = false;
									std::cout << toIp << " not found in blockedIpLocal" << std::endl;
								}
							}
							
							
							if(inet_pton(AF_INET, toIp, &(tempIP.sin_addr)) == 1 && isPresent ){
								if(send(socketFd, sendMsg, strlen(sendMsg), 0) == strlen(sendMsg)){
									cse4589_print_and_log("[%s:SUCCESS]\n", cmd);
									cse4589_print_and_log("[%s:END]\n", cmd);
                            	}
							}
							else{
								cse4589_print_and_log("[%s:ERROR]\n", cmd);
								cse4589_print_and_log("[%s:END]\n", cmd);
							}
						}
						else if((strncmp(command, "SEND", 4) == 0) && isLoggedIn){
							char* tokens = strtok(command, " ");
							char * cmd = tokens;
							tokens = strtok(NULL, " ");
							char * ip = tokens;
							tokens = strtok(NULL, "");
							char * msg = tokens;
							printf("%s",ip);
							printf("%s",msg);
							struct sockaddr_in tempIP;
							char * clientIP = fetchIP();
							char * sendMsg = (char*) malloc(sizeof(char)*MSG_SIZE);
							strcpy(sendMsg, cmd);
							strcat(sendMsg, " ");
							strcat(sendMsg, clientIP);
							strcat(sendMsg, " ");
							strcat(sendMsg, ip);
							strcat(sendMsg, " ");
							strcat(sendMsg, msg);
							printf("\n%s",sendMsg);
							nodeClient* temp = headNode;
							bool isValid = false;
							while(temp != NULL){
								if(strcmp(temp->ipAddress, ip) == 0){
									if(temp->loginStatus == 1){
										isValid = true;
									}
								}
								temp = temp->next;
							}
                            if(inet_pton(AF_INET, ip, &(tempIP.sin_addr)) == 1 && isValid){
								if(send(socketFd, sendMsg, strlen(sendMsg), 0) == strlen(sendMsg)){
									cse4589_print_and_log("[%s:SUCCESS]\n", cmd);
									cse4589_print_and_log("[%s:END]\n", cmd);
                            	}
								else{
									cse4589_print_and_log("[%s:ERROR]\n", cmd);
									cse4589_print_and_log("[%s:END]\n", cmd);
								}
							}
							else{
								cse4589_print_and_log("[%s:ERROR]\n", cmd);
								cse4589_print_and_log("[%s:END]\n", cmd);
							}
                        }
						else if((strncmp(command, "BROADCAST", 9) == 0) && isLoggedIn){
							char* tokens = strtok(command, " ");
							char * cmd = tokens;
							tokens = strtok(NULL, "");
							char * msg = tokens;
							char * sendMsg = (char*) malloc(sizeof(char)*MSG_SIZE);
							strcpy(sendMsg, cmd);
							strcat(sendMsg, " ");
							strcat(sendMsg, client_IP);
							strcat(sendMsg, " ");
							strcat(sendMsg, msg);
							if(send(socketFd, sendMsg, strlen(sendMsg), 0) == strlen(sendMsg)){
								cse4589_print_and_log("[%s:SUCCESS]\n", cmd);
								cse4589_print_and_log("[%s:END]\n", cmd);
							}
							else{
								cse4589_print_and_log("[%s:ERROR]\n", cmd);
								cse4589_print_and_log("[%s:END]\n", cmd);
							}
						}
						else if(strcmp(command, "DETAILS") == 0){
							struct nodeClient *temp = headNode;
							int n = 1;
							while(temp != NULL){
								// cse4589_print_and_log("%-5d%-35s%-20s%-8d%-8d%-8d\n", n, temp->hostName, temp->ipAddress, temp->portNo, temp->socketFd, temp->loginStatus);
								temp = temp->next;
								n++;
							}
						}
                        else{
                            printf("Wrong command");
                        }
						for (const char* ip : blockedIpLocal) {
						std::cout << "before free block: " << ip << std::endl;
						}
                        // free(command);
						for (const char* ip : blockedIpLocal) {
						std::cout << "after free block: " << ip << std::endl;
						}
                    }
                	/* Read from existing clients */
                    else if(sock_index == socketFd){
                        /* Initialize buffer to receieve response */
                        char *buffer = (char*) malloc(sizeof(char)*BUFFER_SIZE);
                        memset(buffer, '\0', BUFFER_SIZE);
                        ssize_t bytesReceived;
                        if (bytesReceived = (recv(sock_index, buffer, BUFFER_SIZE, 0) > 0)){
                            //Process incoming data from existing clients here ...
							buffer[strlen(buffer)] = '\0';
                            char *clientList = (char*) malloc(sizeof(char)*BUFFER_SIZE);
                            char *refreshList = (char*) malloc(sizeof(char)*BUFFER_SIZE);
                            
                            memset(clientList, '\0', BUFFER_SIZE);
                            memset(refreshList, '\0', BUFFER_SIZE);

                            printf("\nClient sent me: %s\n", buffer);
                            printf("ECHOing it back to the remote host ... ");

							if(strncmp(buffer, "clientList", 10) == 0){
								headNode = NULL;
                                createNodesFromList(buffer);
								fflush(stdout);
							}
							if(strncmp(buffer, "send", 4) == 0){
								char* tokens = strtok(buffer, " ");
								char * cmd = tokens;
								tokens = strtok(NULL, " ");
								char * fromIp = tokens;
								tokens = strtok(NULL, "");
								char * msg = tokens;
								if(msg != NULL){
									cse4589_print_and_log("[RECEIVED:SUCCESS]\n");
									cse4589_print_and_log("msg from:%s\n[msg]:%s\n", fromIp, msg);
									cse4589_print_and_log("[RECEIVED:END]\n");
								}
								else{
									cse4589_print_and_log("[RECEIVED:ERROR]\n");
									cse4589_print_and_log("[RECEIVED:END]\n");
								}
								// tokens = strtok(msg, "SEND");
								// cmd = tokens;
								// tokens = strtok(NULL, " ");
								// fromIp = tokens;
								// tokens = strtok(NULL, "");
								// msg = tokens;
								// if(msg != NULL){
								// 	cse4589_print_and_log("\n[RECEIVED:SUCCESS]\n");
								// 	cse4589_print_and_log("msg from:%s\n[msg]:%s\n", fromIp, msg);
								// 	cse4589_print_and_log("[RECEIVED:END]\n");
								// }
								// else{
								// 	cse4589_print_and_log("\n[RECEIVED:ERROR]\n");
								// 	cse4589_print_and_log("[RECEIVED:END]\n");
								// }
							}
                        }
                        free(buffer);
                    }
					for (const char* ip : blockedIpLocal) {
						std::cout << "after block: " << ip << std::endl;
					}
                }
            }
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
		element >> arg;
		arguments.push_back(arg);
		element >> arg;
		arguments.push_back(arg);

		struct nodeClient *dataNode = (struct nodeClient*) malloc(sizeof(struct nodeClient));
		strcpy(dataNode->hostName, arguments[0].c_str());
		strcpy(dataNode->ipAddress, arguments[1].c_str());
		dataNode->portNo = atoi(arguments[2].c_str());
		dataNode->socketFd = atoi(arguments[3].c_str());
		dataNode->loginStatus = atoi(arguments[4].c_str());
		
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
void createClient(char *clientInfo, int socketFd){
	vector<string> arguments;
	stringstream element(clientInfo);
	string arg, cmd;
	element >> arg;

	element >> arg;
	arguments.push_back(arg);
	element >> arg;
	arguments.push_back(arg);
	element >> arg;
	arguments.push_back(arg);

	struct nodeClient* t = headNode;
	bool existingClient = false;
	
	while(t != NULL){
		std::string str = std::to_string(t->portNo);
    	const char* charPtr = str.c_str();
		cout<<"port: "<<str<<endl;
		cout<<"arg: "<<arguments[2]<<endl;
		if(strcmp(t->ipAddress, arguments[1].c_str()) == 0 && strcmp(charPtr,arguments[2].c_str())==0){
			t->loginStatus = 1;
			existingClient = true;
			if(!t->bufferedMsgs.empty()){
				for(const auto& msg : t->bufferedMsgs){
					char* msgCstr = new char[msg.length() + 1];
					strcpy(msgCstr, msg.c_str());
					char* tokens = strtok(msgCstr, " ");
					char * command = tokens;
					tokens = strtok(NULL, " ");
					char * fromIP = tokens;
					tokens = strtok(NULL, "");
					char * message = tokens;
					char * dataToSend = (char*) malloc(sizeof(char)*MSG_SIZE);
					strcpy(dataToSend, "send ");
					strcat(dataToSend, fromIP);
					strcat(dataToSend, " ");
					strcat(dataToSend, message);
					cout<<msgCstr<<endl;
					if(send(t->socketFd, dataToSend, strlen(dataToSend), 0) != strlen(dataToSend)){
						cse4589_print_and_log("[RELAYED:ERROR]\n");
						cse4589_print_and_log("[RELAYED:END]\n");
					}
					else{
						cse4589_print_and_log("[RELAYED:SUCCESS]\n");
						cse4589_print_and_log("msg from:%s, to:%s\n[msg]:%s\n", fromIP, t->ipAddress, message);
						cse4589_print_and_log("[RELAYED:END]\n");
						t->msgReceived += 1;
						usleep(1000000);
					}
					delete[] msgCstr;
				}
				t->bufferedMsgs.clear();
			}
		}
		t = t->next;
	}
	if(!existingClient){
		struct nodeClient *dataNode = (struct nodeClient*) malloc(sizeof(struct nodeClient));
		strcpy(dataNode->hostName, arguments[0].c_str());
		strcpy(dataNode->ipAddress, arguments[1].c_str());
		dataNode->portNo = atoi(arguments[2].c_str());
		dataNode->socketFd = socketFd;
		dataNode->loginStatus = 1;
		cout<<"dataNode ip: "<<dataNode->ipAddress<<endl;
		dataNode->next = NULL;
		if(headNode == NULL)
			headNode = dataNode;
		else{
			struct nodeClient *temp = headNode;
			
			while(temp->next != NULL){
				temp = temp->next;
			}
			cout<<"port: "<<dataNode->portNo<<endl;
			temp->next = dataNode;
		}
	}
	arguments.clear();
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
void serverDisplayClientDetails(char *cmd){
	cse4589_print_and_log("[%s:SUCCESS]\n", cmd);

	struct nodeClient *temp = headNode;
	int n = 1;
	while(temp != NULL){
		if(temp->loginStatus == 1){
			cse4589_print_and_log("%-5d%-35s%-20s%-8d\n", n, temp->hostName, temp->ipAddress, temp->portNo);
			n++;
		}
		temp = temp->next;
	}

	cse4589_print_and_log("[%s:END]\n", cmd);
}
void displayStatsOfClients(char *cmd){
	cse4589_print_and_log("[%s:SUCCESS]\n", cmd);

	struct nodeClient *temp = headNode;
	int n = 1;
	while(temp != NULL){
		const char* loginStatus = temp->loginStatus == 1 ? "logged-in" : "logged-out";
		cse4589_print_and_log("%-5d%-35s%-8d%-8d%-8s\n", n, temp->hostName, temp->msgSent, temp->msgReceived, loginStatus);
		temp = temp->next;
		n++;
	}
	cse4589_print_and_log("[%s:END]\n", cmd);
}
void displayBLockedClientDetails(char *cmd){
	struct sockaddr_in tempIP;
	
	string msg = cmd;
	int ptr = msg.find(" ");
	string ip_add = msg.substr(ptr+1);
	cout<<"ip_add: "<<ip_add<<endl;
	
	nodeClient *temp = headNode;
	vector<string> blockList;
	bool isPresent = true;
	while(temp!=NULL){
		cout<<temp->ipAddress<<endl;
		if(strcmp(temp->ipAddress,ip_add.c_str())==0){
			blockList.assign(temp->blockedIPs.begin(), temp->blockedIPs.end());
			isPresent = false;
		}
		temp = temp->next;
	}

	for (const std::string& word : blockList) {
        std::cout<<"blocklist vector: " << word << std::endl;
    }
	// struct nodeClient *temp = headNode;
	temp = headNode;
	cout<<"ipPresent: "<<isPresent<<endl;
	cout<<"check: "<<inet_pton(AF_INET, ip_add.c_str(), &(tempIP.sin_addr))<<endl;
	if(inet_pton(AF_INET, ip_add.c_str(), &(tempIP.sin_addr)) == 0 || isPresent){
		cse4589_print_and_log("[%s:ERROR]\n", "BLOCKED");
		cse4589_print_and_log("[%s:END]\n", "BLOCKED");
		return;
	}
	sortClientDetails(); 
	int n = 1; 
	cse4589_print_and_log("[%s:SUCCESS]\n", "BLOCKED");
	while(temp!=NULL){
		string str_ip(temp->ipAddress);
		// cout<<"str_ip: "<<str_ip<<endl;
		auto it = std::find(blockList.begin(), blockList.end(), str_ip);
		if (blockList.size() == 1) {
			// Erase the single element in blockedIPs
			cse4589_print_and_log("%-5d%-35s%-20s%-8d\n", n, temp->hostName, temp->ipAddress, temp->portNo);
		}else{
				if (it != blockList.end()) {
					// std::cout << str_ip << " is present in the vector." << std::endl;
					cse4589_print_and_log("%-5d%-35s%-20s%-8d\n", n, temp->hostName, temp->ipAddress, temp->portNo);
					n++;
    			}
		}
		temp = temp->next;
	}
	cse4589_print_and_log("[%s:END]\n", "BLOCKED");
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
				swap(temp->msgSent, temp->next->msgSent);
				swap(temp->msgReceived, temp->next->msgReceived);
				swap(temp->loginStatus, temp->next->loginStatus);
				swap(temp->socketFd, temp->next->socketFd);
				swap(temp->blockedIPs, temp->next->blockedIPs);
				swap(temp->bufferedMsgs, temp->next->bufferedMsgs);
				
                swapped = 1; 
            } 
            temp = temp->next; 
        } 
        ptr = temp; 
    } while (swapped); 
}
char* clientDetailsFromNodes(){
	char* listOfClients = NULL; 
	char port[8], socketFd[8], loginStatus[8];
	strDynamicConcat(&listOfClients, "clientList ");
	struct nodeClient *temp = headNode;
	// nodeClient* temp = headNode;
	// while(temp!=NULL){
	// 	cout<<temp->ipAddress<<endl;
	// 		for (const char* message : temp->blockedIPs) {
	// 		std::cout<<"clientDetailsFromNodes messages before: " << message << std::endl;
	// 	}
	// 	temp = temp->next;
	// }
	// temp = headNode;
	while(temp != NULL){
		strDynamicConcat(&listOfClients, temp->hostName);
		strDynamicConcat(&listOfClients, " ");
		strDynamicConcat(&listOfClients, temp->ipAddress);
		strDynamicConcat(&listOfClients, " ");
		sprintf(port, "%d", temp->portNo);
		strDynamicConcat(&listOfClients, port);
		strDynamicConcat(&listOfClients, " ");
		sprintf(socketFd, "%d", temp->socketFd);
		strDynamicConcat(&listOfClients, socketFd);
		strDynamicConcat(&listOfClients, " ");
		sprintf(loginStatus, "%d", temp->loginStatus);
		strDynamicConcat(&listOfClients, loginStatus);
		strDynamicConcat(&listOfClients, " ");
		temp = temp->next;
	}
	//printf("%s", listOfClients);
	// struct nodeClient *temp = headNode;
	// temp = headNode;
	// while(temp!=NULL){
	// 	cout<<temp->ipAddress<<endl;
	// 		for (const char* message : temp->blockedIPs) {
	// 		std::cout<<"clientDetailsFromNodes messages after: " << message << std::endl;
	// 	}
	// 	temp = temp->next;
	// }
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
int isIPLoggedIn(char* ip){
	nodeClient* temp = headNode;
	while(temp != NULL){
		if(strcmp(temp->ipAddress, ip) == 0){
			return temp->loginStatus;
		}
		temp = temp->next;
	}
	return 0;
}
void updateLoggedIn(char* ip){
	nodeClient* temp = headNode;
	while(temp != NULL){
		if(strcmp(temp->ipAddress, ip) == 0){
			temp->loginStatus = 1 ;
		}
		temp = temp->next;
	}
}


//     // Sender not found, or headNode is nullptr
//     return 0;
// }

void sendOrBuffer(char* fromIP, char* toIP, char* msg){
	char * dataToSend = (char*) malloc(sizeof(char)*MSG_SIZE);
	strcpy(dataToSend, "send ");
	strcat(dataToSend, fromIP);
	strcat(dataToSend, " ");
	strcat(dataToSend, msg);
	nodeClient *temp = headNode;

	while(temp != NULL){
		if(strcmp(temp->ipAddress, toIP) == 0){
			if(temp->loginStatus == 1){
				if(send(temp->socketFd, dataToSend, strlen(dataToSend), 0) == strlen(dataToSend)){
					cse4589_print_and_log("[RELAYED:SUCCESS]\n");
					cse4589_print_and_log("msg from:%s, to:%s\n[msg]:%s\n", fromIP, toIP, msg);
					cse4589_print_and_log("[RELAYED:END]\n");
					temp->msgReceived += 1;
				}
				else{
					cse4589_print_and_log("[RELAYED:ERROR]\n");
					cse4589_print_and_log("[RELAYED:END]\n");
				}
			}
			else if(temp->loginStatus == 0){

				temp->bufferedMsgs.push_back(dataToSend);
			}
		}
		temp = temp->next;
	}
	nodeClient *t = headNode;
	while(t != NULL){
		if(strcmp(t->ipAddress, fromIP) == 0){
			t->msgSent += 1;
		}
		t = t->next;
	}
}
void broadcastOrBuffer(char* fromIP, char* toIP, char* msg){
	char * dataToSend = (char*) malloc(sizeof(char)*MSG_SIZE);
	strcpy(dataToSend, "send ");
	strcat(dataToSend, fromIP);
	strcat(dataToSend, " ");
	strcat(dataToSend, msg);

	nodeClient* node = headNode;
	vector<char*> blocked;
	while(node != NULL){
		for(char* ip: node->blockedIPs){
			if(strcmp(ip, fromIP) == 0){
				blocked.push_back(node->ipAddress);
			}
		}
		node = node->next;
	}

	nodeClient *temp = headNode;
	bool sendSuccess = false;

	while(temp != NULL){
		if(strcmp(temp->ipAddress, fromIP) != 0){
			bool isBlocked = false;
			for(char* bIP: blocked){
				if(strcmp(temp->ipAddress, bIP) == 0){
					isBlocked = true;
				}
			}
			if(!isBlocked){
				if(temp->loginStatus == 1){
					if(send(temp->socketFd, dataToSend, strlen(dataToSend), 0) == strlen(dataToSend)){
						cse4589_print_and_log("[RELAYED:SUCCESS]\n");
						cse4589_print_and_log("msg from:%s, to:%s\n[msg]:%s\n", fromIP, toIP, msg);
						cse4589_print_and_log("[RELAYED:END]\n");
						sendSuccess = true;
						temp->msgReceived += 1;
					}
					else{
						cse4589_print_and_log("[RELAYED:ERROR]\n");
						cse4589_print_and_log("[RELAYED:END]\n");
					}
				}
				else if(temp->loginStatus == 0){
					temp->bufferedMsgs.push_back(dataToSend);
				}
			}
		}
		temp = temp->next;
	}
	if(sendSuccess){
		nodeClient *temp = headNode;
		while(temp != NULL){
			if(strcmp(temp->ipAddress, fromIP) == 0){
				temp->msgSent += 1;
			}
			temp = temp->next;
		}
	}
}
int isIPBlocked(char* fromIP, char* toIP){
	printf("in is IP blocked");
	nodeClient* temp = headNode;
	while(temp != NULL){
		if(strcmp(temp->ipAddress, toIP) == 0){
			for(char* ip: temp->blockedIPs){
				if(strcmp(ip, fromIP) == 0){
					printf("--returning 1");
					return 1;
					
				}
			}
		}
		temp = temp->next;
	}
	printf("--returning zero");
	return 0;
	
}