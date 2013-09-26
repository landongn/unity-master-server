#include "NativeFeatureIncludes.h"
#if _RAKNET_SUPPORT_LightweightDatabaseServer==1

#include "LightweightDatabaseServer.h"
#include "MessageIdentifiers.h"
#include "BitStream.h"
#include "StringCompressor.h"
#include "RakPeerInterface.h"
#include "TableSerializer.h"
#include "RakAssert.h"
#include "GetTime.h"
#include "Rand.h"
#include "MasterServer.h"
#include "Log.h"
#include "DS_Table.h"
#include <sstream>
#include "RakNetStatistics.h"
#include <time.h>
static const int SEND_PING_INTERVAL=15000;
static const int DROP_SERVER_INTERVAL=75000;

#ifdef _MSC_VER
#pragma warning( push )
#endif

int LightweightDatabaseServer::DatabaseTableComp( const char* const &key1, const char* const &key2 )
{
	return strcmp(key1, key2);
}

LightweightDatabaseServer::LightweightDatabaseServer()
{
	// Set current master server _client_ version
	version[0]=2; version[1]=0; version[2]=0;

	statDelay = 0;
	statTimer = time(0) + statDelay;
	lastMinQueryCount = 0;
	lastMinUpdateCount = 0;
}
LightweightDatabaseServer::~LightweightDatabaseServer()
{
	Clear();
}
DataStructures::Table *LightweightDatabaseServer::GetTable(const char *tableName)
{
	if (tableName==0)
	{
		if (database.Size()>0)
			return &(database[0]->table);
		return 0;
	}
	if (database.Has(tableName))
		return &(database.Get(tableName)->table);
	return 0;
}
DataStructures::Page<unsigned, DataStructures::Table::Row*, _TABLE_BPLUS_TREE_ORDER> *LightweightDatabaseServer::GetTableRows(const char *tableName)
{
	if (database.Has(tableName))
		database.Get(tableName)->table.GetRows().GetListHead();
	return 0;
}
DataStructures::Table* LightweightDatabaseServer::AddTable(const char *tableName,
																			  bool allowRemoteQuery,
																			  bool allowRemoteUpdate,
																			  bool allowRemoteRemove,
																			  const char *queryPassword,
																			  const char *updatePassword,
																			  const char *removePassword,
																			  bool oneRowPerSystemAddress,
																			  bool onlyUpdateOwnRows,
																			  bool removeRowOnPingFailure,
																			  bool removeRowOnDisconnect,
																			  bool autogenerateRowIDs)
{
	if (tableName==0 || tableName[0]==0)
		return 0;
	if (database.Has(tableName))
		return 0;
	DatabaseTable *databaseTable = RakNet::OP_NEW<DatabaseTable>( __FILE__, __LINE__ );

	strncpy(databaseTable->tableName, tableName, _SIMPLE_DATABASE_TABLE_NAME_LENGTH-1);
	databaseTable->tableName[_SIMPLE_DATABASE_TABLE_NAME_LENGTH-1]=0;

	if (allowRemoteUpdate)
	{
		strncpy(databaseTable->updatePassword, updatePassword, _SIMPLE_DATABASE_PASSWORD_LENGTH-1);
		databaseTable->updatePassword[_SIMPLE_DATABASE_PASSWORD_LENGTH-1]=0;
	}
	else
		databaseTable->updatePassword[0]=0;

	if (allowRemoteQuery)
	{
		strncpy(databaseTable->queryPassword, queryPassword, _SIMPLE_DATABASE_PASSWORD_LENGTH-1);
		databaseTable->queryPassword[_SIMPLE_DATABASE_PASSWORD_LENGTH-1]=0;
	}
	else
		databaseTable->queryPassword[0]=0;

	if (allowRemoteRemove)
	{
		strncpy(databaseTable->removePassword, removePassword, _SIMPLE_DATABASE_PASSWORD_LENGTH-1);
		databaseTable->removePassword[_SIMPLE_DATABASE_PASSWORD_LENGTH-1]=0;
	}
	else
		databaseTable->removePassword[0]=0;

	if (allowRemoteUpdate)
	{
		databaseTable->allowRemoteUpdate=true;
		databaseTable->oneRowPerSystemAddress=oneRowPerSystemAddress;
		databaseTable->onlyUpdateOwnRows=onlyUpdateOwnRows;
		databaseTable->removeRowOnPingFailure=removeRowOnPingFailure;
		databaseTable->removeRowOnDisconnect=removeRowOnDisconnect;
	}
	else
	{
		// All these parameters are related to IP tracking, which is not done if remote updates are not allowed
		databaseTable->allowRemoteUpdate=true;
		databaseTable->oneRowPerSystemAddress=false;
		databaseTable->onlyUpdateOwnRows=false;
		databaseTable->removeRowOnPingFailure=false;
		databaseTable->removeRowOnDisconnect=false;
	}

	databaseTable->nextRowId=0;
	databaseTable->nextRowPingCheck=0;

	databaseTable->autogenerateRowIDs=autogenerateRowIDs;
	databaseTable->allowRemoteRemove=allowRemoteRemove;
	databaseTable->allowRemoteQuery=allowRemoteQuery;

	database.SetNew(databaseTable->tableName, databaseTable);

	if ( oneRowPerSystemAddress || onlyUpdateOwnRows || removeRowOnPingFailure || removeRowOnDisconnect)
	{
		databaseTable->SystemAddressColumnIndex=databaseTable->table.AddColumn(SYSTEM_ID_COLUMN_NAME, DataStructures::Table::BINARY);
		databaseTable->SystemGuidColumnIndex=databaseTable->table.AddColumn(SYSTEM_GUID_COLUMN_NAME, DataStructures::Table::BINARY);
	}
	else
	{
		databaseTable->SystemAddressColumnIndex=(unsigned) -1;
		databaseTable->SystemGuidColumnIndex=(unsigned) -1;
	}
	if (databaseTable->removeRowOnPingFailure)
	{
		databaseTable->lastPingResponseColumnIndex=databaseTable->table.AddColumn(LAST_PING_RESPONSE_COLUMN_NAME, DataStructures::Table::NUMERIC);
		databaseTable->nextPingSendColumnIndex=databaseTable->table.AddColumn(NEXT_PING_SEND_COLUMN_NAME, DataStructures::Table::NUMERIC);
	}
	else
	{
		databaseTable->lastPingResponseColumnIndex=(unsigned) -1;
		databaseTable->nextPingSendColumnIndex=(unsigned) -1;
	}

	return &(databaseTable->table);
}
bool LightweightDatabaseServer::RemoveTable(const char *tableName)
{
	LightweightDatabaseServer::DatabaseTable *databaseTable;
	databaseTable = database.Get(tableName);
	if (databaseTable==0)
		return false;
	// Be sure to call Delete on database before I do the actual pointer deletion since the key won't be valid after that time.
	database.Delete(tableName);
	databaseTable->table.Clear();
	RakNet::OP_DELETE(databaseTable, __FILE__, __LINE__);
	return true;
}
void LightweightDatabaseServer::Clear(void)
{
	unsigned i;

	for (i=0; i < database.Size(); i++)
	{
		database[i]->table.Clear();
		RakNet::OP_DELETE(database[i], __FILE__, __LINE__);
	}

	database.Clear();
}
unsigned LightweightDatabaseServer::GetAndIncrementRowID(const char *tableName)
	{
	LightweightDatabaseServer::DatabaseTable *databaseTable;
	databaseTable = database.Get(tableName);
	RakAssert(databaseTable);
	RakAssert(databaseTable->autogenerateRowIDs==true);
	return ++(databaseTable->nextRowId) - 1;
	}

void LightweightDatabaseServer::Update(void)
{
	RakNetTime rakTime=0;
	DatabaseTable *databaseTable;
	DataStructures::Page<unsigned, DataStructures::Table::Row*, _TABLE_BPLUS_TREE_ORDER> *cur;
	unsigned i,j;
	DataStructures::Table::Row* row;
	DataStructures::List<unsigned> removeList;
	SystemAddress systemAddress;

	// periodic ping if removing system that do not respond to pings.
	for (i=0; i < database.Size(); i++)
	{
		databaseTable=database[i];

		if (databaseTable->removeRowOnPingFailure)
		{
			// Reading the time is slow - only do it once if necessary.
			if (rakTime==0)
				rakTime = RakNet::GetTime();

			if (databaseTable->nextRowPingCheck < rakTime)
			{
				databaseTable->nextRowPingCheck=rakTime+1000+(randomMT()%1000);
				const DataStructures::BPlusTree<unsigned, DataStructures::Table::Row*, _TABLE_BPLUS_TREE_ORDER> &rows = databaseTable->table.GetRows();
				cur = rows.GetListHead();
				DataStructures::List<SystemAddress> removeAddressList;
				while (cur)
				{
					// Mark dropped entities
					for (j=0; j < (unsigned)cur->size; j++)
					{
						row = cur->data[j];
						row->cells[databaseTable->SystemAddressColumnIndex]->Get((char*)&systemAddress, 0);
						if (rakPeerInterface->IsConnected(systemAddress)==false)
						{
							if (rakTime > rakTime - (unsigned int) row->cells[databaseTable->lastPingResponseColumnIndex]->i &&
								rakTime - (unsigned int) row->cells[databaseTable->lastPingResponseColumnIndex]->i > (unsigned int) DROP_SERVER_INTERVAL)
							{
								removeList.Insert(cur->keys[j], __FILE__, __LINE__);
								removeAddressList.Insert(systemAddress, __FILE__, __LINE__);
							}
							else
							{
								if (row->cells[databaseTable->nextPingSendColumnIndex]->i < (int) rakTime)
								{
									char str1[64];
									systemAddress.ToString(false, str1);
									rakPeerInterface->Ping(str1, systemAddress.port, false);
									row->cells[databaseTable->nextPingSendColumnIndex]->i=(double)(rakTime+SEND_PING_INTERVAL+(randomMT()%1000));
								}
							}
						}
					}
					cur=cur->next;
				}

				// Remove dropped entities
				for (j=0; j < removeList.Size(); j++)
				{
					addressMap.erase(removeAddressList[j]);
					databaseTable->table.RemoveRow(removeList[j]);
				}
				removeList.Clear(true, __FILE__,__LINE__);

			}
		}
	}
	
	// Total queries and updates at COUNT_DELAY interval
	if (statTimer < time(0) && statDelay > 0)
	{
		std::ostringstream tableString;
		int emptyTables = 0;
		for (i=0; i < database.Size(); i++)
		{
			databaseTable=database[i];
			tableString << databaseTable->tableName << " ";
			if (databaseTable->table.GetRowCount() == 0)
				emptyTables++;
		}
		if (database.Size() > 0)
		{
			tableString << "\n";
			Log::stats_log(tableString.str().c_str());
		}
			
		if (rakPeerInterface->NumberOfConnections() == 0 && addressMap.size() != 0)
		{
			Log::warn_log("Cleaning up address map, dumping %d addresses\n", addressMap.size());
			addressMap.clear();
		}	

		Log::stats_log("Tables: %d, Empty tables: %d, Conn. count: %d, Queries: %d, Updates: %d, Address map: %d\n", database.Size(), emptyTables, rakPeerInterface->NumberOfConnections(), lastMinQueryCount, lastMinUpdateCount, addressMap.size());
		// Reset last minute counters if minute is up
		lastMinQueryCount = 0;
		lastMinUpdateCount = 0;
		statTimer = time(0) + statDelay;
	}
}
PluginReceiveResult LightweightDatabaseServer::OnReceive(Packet *packet)
	{
	switch (packet->data[0])
		{
		case ID_DATABASE_QUERY_REQUEST:
			Log::info_log("Query request from %s\n", packet->systemAddress.ToString());
			lastMinQueryCount++;
			OnQueryRequest(packet);
			return RR_STOP_PROCESSING_AND_DEALLOCATE;
		case ID_DATABASE_UPDATE_ROW:
			Log::info_log("Update row request from %s\n", packet->systemAddress.ToString());
			lastMinUpdateCount++;
			OnUpdateRow(packet);
			return RR_STOP_PROCESSING_AND_DEALLOCATE;
		case ID_DATABASE_REMOVE_ROW:
			OnRemoveRow(packet);
			return RR_STOP_PROCESSING_AND_DEALLOCATE;
		case ID_PONG:
			OnPong(packet);
			return RR_CONTINUE_PROCESSING;
		}
	return RR_CONTINUE_PROCESSING;
	}
void LightweightDatabaseServer::OnClosedConnection(SystemAddress systemAddress, RakNetGUID rakNetGUID, PI2_LostConnectionReason lostConnectionReason )
{
	(void) rakNetGUID;
	(void) lostConnectionReason;
	Log::info_log("Connection with %s closed\n", systemAddress.ToString());

	RemoveRowsFromIP(systemAddress);
}
void LightweightDatabaseServer::OnQueryRequest(Packet *packet)
{
	RakNet::BitStream inBitstream(packet->data, packet->length, false);

	if (!CheckVersion(packet, inBitstream))
	{
		// Incompatible version, stop processing
		return;
	}
	LightweightDatabaseServer::DatabaseTable *databaseTable = DeserializeClientHeader(&inBitstream, packet, 0, true);
	RakNet::BitStream outBitstream;
	if (databaseTable==0)
	{
		// Return empty list to client so he knows for sure no servers are running
		outBitstream.Write((MessageID)ID_DATABASE_QUERY_REPLY);
		rakPeerInterface->Send(&outBitstream, HIGH_PRIORITY, RELIABLE_ORDERED, 0, packet->systemAddress, false);			
		return;
	}
	else if (databaseTable->table.GetRowCount() == 0)
	{
		outBitstream.Write((MessageID)ID_DATABASE_QUERY_REPLY);
		rakPeerInterface->Send(&outBitstream, HIGH_PRIORITY, RELIABLE_ORDERED, 0, packet->systemAddress, false);			
		return;
	}
	if (databaseTable->allowRemoteQuery==false)
		return;
	unsigned char numColumnSubset;
	unsigned i;
	if (inBitstream.Read(numColumnSubset)==false)
		return;
	unsigned char columnName[256];
	unsigned columnIndicesSubset[256];
	unsigned columnIndicesCount;
	for (i=0,columnIndicesCount=0; i < numColumnSubset; i++)
		{
		stringCompressor->DecodeString((char*)columnName, 256, &inBitstream);
		unsigned colIndex = databaseTable->table.ColumnIndex((char*)columnName);
		if (colIndex!=(unsigned)-1)
			columnIndicesSubset[columnIndicesCount++]=colIndex;
		}
	unsigned char numNetworkedFilters;
	if (inBitstream.Read(numNetworkedFilters)==false)
		return;
	DatabaseFilter networkedFilters[256];
	for (i=0; i < numNetworkedFilters; i++)
		{
		if (networkedFilters[i].Deserialize(&inBitstream)==false)
			return;
		}

	unsigned rowIds[256];
	unsigned char numRowIDs;
	if (inBitstream.Read(numRowIDs)==false)
		return;
	for (i=0; i < numRowIDs; i++)
		inBitstream.Read(rowIds[i]);

	// Convert the safer and more robust networked database filter to the more efficient form the table actually uses.
	DataStructures::Table::FilterQuery tableFilters[256];
	unsigned numTableFilters=0;
	for (i=0; i < numNetworkedFilters; i++)
		{
		tableFilters[numTableFilters].columnIndex=databaseTable->table.ColumnIndex(networkedFilters[i].columnName);
		if (tableFilters[numTableFilters].columnIndex==(unsigned)-1)
			continue;
		if (networkedFilters[i].columnType!=databaseTable->table.GetColumns()[tableFilters[numTableFilters].columnIndex].columnType)
			continue;
		tableFilters[numTableFilters].operation=networkedFilters[i].operation;
		// It's important that I store a pointer to the class here or the destructor of the class will deallocate the cell twice
		tableFilters[numTableFilters++].cellValue=&(networkedFilters[i].cellValue);
		}

	DataStructures::Table queryResult;
	databaseTable->table.QueryTable(columnIndicesSubset, columnIndicesCount, tableFilters, numTableFilters, rowIds, numRowIDs, &queryResult);
	
	// Go through query results and check if the IP address needs to be replaced because of common NAT router.
	DataStructures::Page<unsigned, DataStructures::Table::Row*, _TABLE_BPLUS_TREE_ORDER> *cur = queryResult.GetRows().GetListHead();
	unsigned rowIndex;
	while (cur)
	{
		for (rowIndex=0; rowIndex < (unsigned)cur->size; rowIndex++)
		{
			// If this host has the same IP as the client requesting the host list give him the internal IPs since 
			// connecting directly to the external IP will probably not work. Do this regardless of if NAT was enabled or not.
			if (strcmp(cur->data[rowIndex]->cells[IPINDEX]->c,packet->systemAddress.ToString(false))==0)
			{
				SystemAddress tempID;
				tempID.SetBinaryAddress(cur->data[rowIndex]->cells[IPINDEX]->c);
				tempID.port = cur->data[rowIndex]->cells[PORTINDEX]->i;

				int ipCount = ((InternalID)addressMap[tempID]).ips.size();
				if (addressMap.count(tempID) > 0)
				{
					char (*ips)[16];
					ips = new char[ipCount][16];
					int counter = 0;
					Log::debug_log("Injecting %d internal IPs: ", ipCount);
					std::ostringstream internalIPString;
					internalIPString << "Injecting %d internal IPs: " << ipCount << " ";
					IPList::iterator i;
					IPList list = ((InternalID)addressMap[tempID]).ips;
					for (i= list.begin(); i != list.end(); i++)
					{
						strcpy(ips[counter], ((std::string)*i).c_str());
						internalIPString << ips[counter] << " ";
						counter++;
					}
					internalIPString << "\n";
					Log::debug_log(internalIPString.str().c_str());
					
					cur->data[rowIndex]->UpdateCell(NATINDEX, 0.0);	// Turn off NAT because we don't need to NAT connection routine internally
					cur->data[rowIndex]->UpdateCell(IPINDEX, ipCount*16, (char*)ips);
					cur->data[rowIndex]->UpdateCell(PORTINDEX, ((InternalID)addressMap[tempID]).port);
					delete[] ips;
				}
				else
				{
					Log::error_log("Server (with NAT enabled) and client IPs, %s, match but server internal IP was not found during lookup\n", packet->systemAddress.ToString(false));
				}
			}
		}
		cur=cur->next;
	}
	outBitstream.Write((MessageID)ID_DATABASE_QUERY_REPLY);
	TableSerializer::SerializeTable(&queryResult, &outBitstream);
	SendUnified(&outBitstream, HIGH_PRIORITY, RELIABLE_ORDERED, 0, packet->systemAddress, false);
}
void LightweightDatabaseServer::OnUpdateRow(Packet *packet)
{
	RakNet::BitStream inBitstream(packet->data, packet->length, false);
	
	if (!CheckVersion(packet, inBitstream))
	{
		// Incompatible version, stop processing
		return;
	}
	
	LightweightDatabaseServer::DatabaseTable *databaseTable = DeserializeClientHeader(&inBitstream, packet, 1);
	if (databaseTable==0)
	{
		Log::error_log("Table not found and failed to dynamically create table\n");
		return;
	}
	if (databaseTable->allowRemoteUpdate==false)
	{
		Log::error_log("Remote updates not allowed by table design\n");
		return;
	}
	unsigned char updateMode;
	bool hasRowId=false;
	unsigned rowId;
	unsigned i;
	DataStructures::Table::Row *row;
	inBitstream.Read(updateMode);
	inBitstream.Read(hasRowId);
	if (hasRowId)
		inBitstream.Read(rowId);
	else
		rowId=0;	// MODIFIED: We would like row IDs starting from 0
		//rowId=(unsigned) -1; // Not used here but remove the debugging check
	unsigned char numCellUpdates;
	if (inBitstream.Read(numCellUpdates)==false)
		return;
	// Read the updates for the row
	DatabaseCellUpdate cellUpdates[256];
	for (i=0; i < numCellUpdates; i++)
	{
		if (cellUpdates[i].Deserialize(&inBitstream)==false)
		{
			Log::error_log("LightweightDatabaseServer::OnUpdateRow cellUpdates deserialize failed i=%i numCellUpdates=%i\n",i,numCellUpdates);
			return;
		}
	}

	if ((RowUpdateMode)updateMode==RUM_UPDATE_EXISTING_ROW)
	{
		if (hasRowId==false)
		{
			unsigned rowKey;
			row = GetRowFromIP(databaseTable, packet->systemAddress, &rowKey);
			if (row==0)
			{
				Log::error_log("Attempting to update existing row without supplying row ID");
				return;
			}
		}
		else
		{

			row = databaseTable->table.GetRowByID(rowId);
			if (row==0 || (databaseTable->onlyUpdateOwnRows && RowHasIP(row, packet->systemAddress, databaseTable->SystemAddressColumnIndex)==false))
			{
				if (row==0)
					Log::error_log("LightweightDatabaseServer::OnUpdateRow row = databaseTable->table.GetRowByID(rowId); row==0\n");
				else
					Log::error_log("Attempting to update someone elses row while in RUM_UPDATE_EXISTING_ROW\n");

				return; // You can't update some other system's row
			}
		}
	}
	else if ((RowUpdateMode)updateMode==RUM_UPDATE_OR_ADD_ROW)
		{
		if (hasRowId)
		{
			Log::info_log("Updating row %u in %s for %s\n", rowId, databaseTable->tableName, packet->systemAddress.ToString());
			row = databaseTable->table.GetRowByID(rowId);
		}
		else
			{
			unsigned rowKey;
			row = GetRowFromIP(databaseTable, packet->systemAddress, &rowKey);
			}

		if (row==0)
		{
			if (hasRowId) Log::warn_log("Failed to find given row ID %u from %s\n",rowId, packet->systemAddress.ToString());
			row=AddRow(databaseTable, packet->systemAddress, packet->guid, hasRowId, rowId);
			// Send the chosen row ID to client
			RakNet::BitStream stream;
			stream.Write((unsigned char)ID_DATABASE_ROWID);
			stream.Write(rowId);
			SendUnified(&stream, HIGH_PRIORITY, RELIABLE_ORDERED, 0, packet->systemAddress, false);
			Log::info_log("Sent row ID %u in %s to host at %s\n", rowId, databaseTable->tableName, packet->systemAddress.ToString());
			if (row==0)
			{
				Log::error_log("Row error while performing RUM_UPDATE_OR_ADD_ROW, row update failed\n");
				return;
			}
		}
		else
			{
			// Existing row
			if (databaseTable->onlyUpdateOwnRows && RowHasIP(row, packet->systemAddress, databaseTable->SystemAddressColumnIndex)==false)
				{
				SystemAddress sysAddr;
				memcpy(&sysAddr, row->cells[databaseTable->SystemAddressColumnIndex]->c, SystemAddress::size());

				char str1[64], str2[64];
				packet->systemAddress.ToString(true, str1);
				sysAddr.ToString(true, str2);
				Log::warn_log("Attempting to update someone elses row, while in RUM_UPDATE_OR_ADD_ROW. caller=%s owner=%s. Creating new row.\n",
					str1, str2);
				
				Log::warn_log("Attempting to update someone elses row, while in RUM_UPDATE_OR_ADD_ROW. Creating new row.\n");
				row=AddRow(databaseTable, packet->systemAddress, packet->guid, false, rowId);
				RakNet::BitStream stream;
				stream.Write((unsigned char)ID_DATABASE_ROWID);
				stream.Write(rowId);
				SendUnified(&stream, HIGH_PRIORITY, RELIABLE_ORDERED, 0, packet->systemAddress, false);
				Log::warn_log("Sent row ID %u to host at %s\n",rowId, packet->systemAddress.ToString());

				//return; // You can't update some other system's row
				}
			}
		}
	else
		{
		RakAssert((RowUpdateMode)updateMode==RUM_ADD_NEW_ROW);

		row=AddRow(databaseTable, packet->systemAddress, packet->guid, hasRowId, rowId);
		Log::info_log("Created new row %u in %s for %s\n", rowId, databaseTable->tableName, packet->systemAddress.ToString());
		
		// Send the chosen row ID to client
		RakNet::BitStream stream;
		stream.Write((unsigned char)ID_DATABASE_ROWID);
		stream.Write(rowId);
		rakPeerInterface->Send(&stream, HIGH_PRIORITY, RELIABLE_ORDERED, 0, packet->systemAddress, false);
		Log::info_log("Sent row ID %u to client %s\n",rowId, packet->systemAddress.ToString());
		
		if (row==0)
		{
			Log::error_log("ERROR: LightweightDatabaseServer::OnUpdateRow updateMode==RUM_ADD_NEW_ROW; row==0\n");
			return;
		}
	}

	unsigned columnIndex;
	bool doNAT = false;
	InternalID InternalID;
	for (i=0; i < numCellUpdates; i++)
	{
		columnIndex=databaseTable->table.ColumnIndex(cellUpdates[i].columnName);
		RakAssert(columnIndex!=(unsigned)-1); // Unknown column name
		if (columnIndex!=(unsigned)-1 &&
			(databaseTable->onlyUpdateOwnRows==false ||
			(columnIndex!=databaseTable->lastPingResponseColumnIndex &&
			columnIndex!=databaseTable->nextPingSendColumnIndex &&
			columnIndex!=databaseTable->SystemAddressColumnIndex &&
			columnIndex!=databaseTable->SystemGuidColumnIndex)))
			{
			if (cellUpdates[i].cellValue.isEmpty)
				row->cells[columnIndex]->Clear();
			else if (cellUpdates[i].columnType==databaseTable->table.GetColumnType(columnIndex))
			{
				if (cellUpdates[i].columnType==DataStructures::Table::NUMERIC)
				{
					if (columnIndex == NATINDEX)
						doNAT = (bool) cellUpdates[i].cellValue.i;
					if (columnIndex == PORTINDEX)
					{
						row->UpdateCell(columnIndex, packet->systemAddress.port);
						InternalID.port = cellUpdates[i].cellValue.i;
						Log::debug_log("Updating, injecting %d at column %d\n", packet->systemAddress.port, columnIndex);
					}
					else
					{
						row->UpdateCell(columnIndex, cellUpdates[i].cellValue.i);
						Log::debug_log("Updating, inserting %d at column %d\n", cellUpdates[i].cellValue.i, columnIndex);
					}
				}
				else if (cellUpdates[i].columnType==DataStructures::Table::BINARY)
				{
					if (columnIndex == IPINDEX)
					{
						// Process external IP
						row->UpdateCell(columnIndex, 16, (char*)packet->systemAddress.ToString(false));
						
						// Process internal IP(s)
						char (*ips)[16];
						ips = new char[((int)cellUpdates[i].cellValue.i)/16][16];
						memcpy(ips, cellUpdates[i].cellValue.c, cellUpdates[i].cellValue.i);
						InternalID.ips.clear();
						for (int j=0; j<(cellUpdates[i].cellValue.i)/16; j++)
						{
							if (ips[j][0] == 0) 
								break;
							InternalID.ips.push_back(ips[j]);
						}
						
						Log::debug_log("Updating, injecting %s at column %d\n",packet->systemAddress.ToString(false), columnIndex);
						delete[] ips;
					}
					else
					{
						row->UpdateCell(columnIndex, cellUpdates[i].cellValue.i, cellUpdates[i].cellValue.c);
					}
				}
				else
				{
					RakAssert(cellUpdates[i].columnType==DataStructures::Table::STRING);
					row->UpdateCell(columnIndex, cellUpdates[i].cellValue.c);
					Log::debug_log("Updating, inserting %s at column %d\n",cellUpdates[i].cellValue.c, columnIndex);
				}
			}
		}
	}
	// Add GUID
	row->UpdateCell(GUIDINDEX, packet->guid.ToString());
	Log::debug_log("Updating, inserting %s at column %d\n",packet->guid.ToString(), GUIDINDEX);
	
	std::ostringstream internalIPString;
	internalIPString << "Internal port is " << InternalID.port << " IPs set as ";
	for (IPList::iterator iterator = InternalID.ips.begin(); iterator != InternalID.ips.end(); iterator++)
	{
		internalIPString << (std::string)*iterator << " ";
	}
	internalIPString << "\n";
	Log::debug_log(internalIPString.str().c_str());
	addressMap[packet->systemAddress] = InternalID;
}
void LightweightDatabaseServer::OnRemoveRow(Packet *packet)
{
	RakNet::BitStream inBitstream(packet->data, packet->length, false);
	LightweightDatabaseServer::DatabaseTable *databaseTable = DeserializeClientHeader(&inBitstream, packet, 0, true);
	if (databaseTable==0)
	{
		Log::info_log("Failed to remove row for %s because table lookup failed\n",packet->systemAddress.ToString());
		return;
	}
	if (databaseTable->allowRemoteRemove==false)
	{
		Log::info_log("Remote row removal on %s forbidden, from %s\n", databaseTable->tableName, packet->systemAddress.ToString());
		return;
	}
	unsigned rowId;
	inBitstream.Read(rowId);
	databaseTable->table.RemoveRow(rowId);
	addressMap.erase(packet->systemAddress);
	Log::info_log("Removing row %u by request\n",rowId);
	Log::debug_log("Current address map count is %d\n", addressMap.size());
}
void LightweightDatabaseServer::OnPong(Packet *packet)
{
	unsigned databaseIndex;
	DatabaseTable *databaseTable;
	unsigned curIndex;
	SystemAddress systemAddress;
	RakNetTime time=0;
	for (databaseIndex=0; databaseIndex < database.Size(); databaseIndex++)
	{
		databaseTable=database[databaseIndex];
		if (databaseTable->removeRowOnPingFailure)
		{
			if ((unsigned int) databaseTable->SystemAddressColumnIndex==(unsigned int)-1)
				continue;
			if (time==0)
				time=RakNet::GetTime();

			const DataStructures::BPlusTree<unsigned, DataStructures::Table::Row*, _TABLE_BPLUS_TREE_ORDER> &rows = databaseTable->table.GetRows();
			DataStructures::Page<unsigned, DataStructures::Table::Row*, _TABLE_BPLUS_TREE_ORDER> *cur = rows.GetListHead();

			while (cur)
			{
				for (curIndex=0; curIndex < (unsigned) cur->size; curIndex++)
				{
					cur->data[curIndex]->cells[databaseTable->SystemAddressColumnIndex]->Get((char*)&systemAddress,0);
					if (systemAddress==packet->systemAddress)
					{
						cur->data[curIndex]->cells[databaseTable->lastPingResponseColumnIndex]->i=(int)time;
					}
				}
				cur=cur->next;
			}
		}
	}
}

// Read version number from bitstream
bool LightweightDatabaseServer::CheckVersion(Packet *packet, RakNet::BitStream &inBitstream)
{
	inBitstream.IgnoreBits(8);
	char peerVersion[3];
	inBitstream.Read(peerVersion, sizeof(peerVersion));

	char version009[3] = {0,0,9};
	if (memcmp(peerVersion, version, sizeof(peerVersion))==0)
	{
		Log::debug_log("Version %d.%d.%d connected\n", peerVersion[0], peerVersion[1], peerVersion[2]);
	}
	// This is just demo code but for each master server version which is redirected, an if block like this needs to be created.
	else if (memcmp(peerVersion, version009, sizeof(peerVersion))==0)
	{
		RakNet::BitStream outBitstream;
		SystemAddress oldMasterServer, oldFacilitator;
		oldMasterServer.port = 34567;
		oldMasterServer.SetBinaryAddress("127.0.0.1");
		oldFacilitator.port = 50001;
		oldFacilitator.SetBinaryAddress("127.0.0.1");
		if (rakPeerInterface->GetInternalID().port == oldMasterServer.port)
		{
			Log::warn_log("Wrong version number but accepting connection\n");
			return true;
		}
		else
		{
			char oldFac[32];
			strcpy(oldFac, oldFacilitator.ToString());
			Log::info_log("Old version connected, %d.%d.%d, redirecting master server to %s, facilitator to %s\n", peerVersion[0], peerVersion[1], peerVersion[2], oldMasterServer.ToString(), oldFac);

			outBitstream.Write((MessageID)ID_MASTERSERVER_REDIRECT);
			outBitstream.Write(oldMasterServer);
			outBitstream.Write(oldFacilitator);
			rakPeerInterface->Send(&outBitstream, HIGH_PRIORITY, RELIABLE_ORDERED, 0, packet->systemAddress, false);			
			return false;
		}
	}
	else
	{
		Log::error_log("Incorrect master server client version, %d.%d.%d\n", peerVersion[0], peerVersion[1], peerVersion[2]);
		
		RakNet::BitStream outBitstream;
		outBitstream.Write((MessageID)ID_MASTERSERVER_MSG);
		std::string message = "Your master server client version is too old. Please upgrade.";
		outBitstream.Write((unsigned int)message.length());
		outBitstream.Write(message.c_str(), (unsigned int)message.length());
		rakPeerInterface->Send(&outBitstream, HIGH_PRIORITY, RELIABLE_ORDERED, 0, packet->systemAddress, false);			
		return false;
	}
	
	// Correct version
	return true;

}

// Added utility function for dynamic table creation
LightweightDatabaseServer::DatabaseTable* LightweightDatabaseServer::CreateDefaultTable(std::string tableName)
{
	if (tableName.empty())
	{
		Log::error_log("Error: Empty table name given during table creation. Aborting.\n");
		return 0;
	}

	bool allowRemoteUpdate = true;
	bool allowRemoteQuery = true;
	bool allowRemoteRemove = true;
	char queryPassword[_SIMPLE_DATABASE_PASSWORD_LENGTH];
	char updatePassword[_SIMPLE_DATABASE_PASSWORD_LENGTH];		
	char removePassword[_SIMPLE_DATABASE_PASSWORD_LENGTH];
	bool oneRowPerSystemId = true;
	bool onlyUpdateOwnRows = true;
	bool removeRowOnPingFailure = true;	
	bool removeRowOnDisconnect = true;
	bool autogenerateRowIDs = true;
		
	queryPassword[0] = 0;
	updatePassword[0] = 0;
	removePassword[0] = 0;
	
	DataStructures::Table *table;	
	table = AddTable(const_cast<char*>(tableName.c_str()), allowRemoteQuery, allowRemoteUpdate, allowRemoteRemove, queryPassword, updatePassword, removePassword, oneRowPerSystemId, onlyUpdateOwnRows, removeRowOnPingFailure, removeRowOnDisconnect, autogenerateRowIDs);
	if (table)
	{
		Log::debug_log("Table %s created.\n", tableName.c_str());
		table->AddColumn("NAT", table->NUMERIC);
		table->AddColumn("Game name", table->STRING);
		table->AddColumn("Connected players", table->NUMERIC);
		table->AddColumn("Player limit", table->NUMERIC);
		table->AddColumn("Password protected", table->NUMERIC);
		table->AddColumn("IP address", table->BINARY);
		table->AddColumn("Port", table->NUMERIC);
		table->AddColumn("Comment", table->STRING);
		table->AddColumn("GUID", table->STRING);
		return database.Get(const_cast<char*>(tableName.c_str()));
	}
	else
	{
		Log::error_log("Table %s creation failed.  Possibly already exists.\n", tableName.c_str());
		return 0;
	}
}

LightweightDatabaseServer::DatabaseTable * LightweightDatabaseServer::DeserializeClientHeader(RakNet::BitStream *inBitstream, Packet *packet, int mode, bool query)
	{
	RakNet::BitStream outBitstream;
	bool hasPassword=false;
	char password[_SIMPLE_DATABASE_PASSWORD_LENGTH];
	//inBitstream->IgnoreBits(8);	// inBitstream should be at the correct position when passed
	char tableName[_SIMPLE_DATABASE_TABLE_NAME_LENGTH];
	stringCompressor->DecodeString(tableName, _SIMPLE_DATABASE_TABLE_NAME_LENGTH, inBitstream);
	if ((tableName[0]) == 0)
	{
		if (query)
			Log::error_log("No table name defined, during query/removal.\n");
		else
			Log::error_log("No table name defined.\n");
		Log::error_log("Malformed packet from %s, packet size %d bits.\n", packet->systemAddress.ToString(), packet->bitSize);
		return 0;
	}
	DatabaseTable *databaseTable;
	// Check if table already exists or if it needs to be dynamically created
	if (database.Has(tableName))
	{
		Log::info_log("Fetching existing table %s\n", tableName);
		databaseTable = database.Get(tableName);
	}
	else
	{
		// If this is a query (row removal or host list) then we should 
		// stop now because we don't want to create a new table etc.
		if (query)
		{
			Log::info_log("Table %s not found during query or row removal from %s\n", tableName, packet->systemAddress.ToString());
			return 0;
		}

		// Note that when table lookups fail index 0 is returned and this is the first table created.
		databaseTable = CreateDefaultTable(tableName);
		if (databaseTable == 0)
		{
			Log::error_log("No table selected. Cannot proceed.\n");
			outBitstream.Write((MessageID)ID_DATABASE_UNKNOWN_TABLE);
			rakPeerInterface->Send(&outBitstream, HIGH_PRIORITY, RELIABLE_ORDERED, 0, packet->systemAddress, false);
			return 0;
		}
	}
	
	if (strcmp(databaseTable->tableName,tableName)!=0)
	{
		Log::warn_log("Incorrect table returned during table lookup, requested %s, got %s\n", tableName, databaseTable->tableName);
		if (query)
			return 0;
		databaseTable = CreateDefaultTable(tableName);
	}
	
	if (databaseTable == 0)
	{
		Log::error_log("Critical error: No database table selected just before being accessed. Something went wrong.\n");
		return 0;
	}

	const char *dbPass;
	if (mode==0)
		dbPass=databaseTable->queryPassword;
	else if (mode==1)
		dbPass=databaseTable->updatePassword;
	else
		dbPass=databaseTable->removePassword;

	inBitstream->Read(hasPassword);
	if (hasPassword)
		{
		if (stringCompressor->DecodeString(password, _SIMPLE_DATABASE_PASSWORD_LENGTH, inBitstream)==false)
			Log::error_log("Error: Table password info not found\n");
			return 0;
			if (databaseTable->queryPassword[0] && strcmp(password, dbPass)!=0)
			{
				outBitstream.Write((MessageID)ID_DATABASE_INCORRECT_PASSWORD);
				rakPeerInterface->Send(&outBitstream, HIGH_PRIORITY, RELIABLE_ORDERED, 0, packet->systemAddress, false);
				// Short ban to prevent brute force password attempts
				char str1[64];
				packet->systemAddress.ToString(false, str1);
				rakPeerInterface->AddToBanList(str1, 1000);
				// Don't send a disconnection notification so it closes the connection right away.
				rakPeerInterface->CloseConnection(packet->systemAddress, false, 0);
				Log::error_log("Incorrect table password given, banning client\n");
				return 0;
			}
		}
	else if (dbPass[0])
		{
		outBitstream.Write((MessageID)ID_DATABASE_INCORRECT_PASSWORD);
		rakPeerInterface->Send(&outBitstream, HIGH_PRIORITY, RELIABLE_ORDERED, 0, packet->systemAddress, false);
			Log::error_log("Error: Empty table password\n");
		return 0;
		}

	return databaseTable;
	}

DataStructures::Table::Row * LightweightDatabaseServer::GetRowFromIP(DatabaseTable *databaseTable, SystemAddress systemAddress, unsigned *rowKey)
{
	const DataStructures::BPlusTree<unsigned, DataStructures::Table::Row*, _TABLE_BPLUS_TREE_ORDER> &rows = databaseTable->table.GetRows();
	DataStructures::Page<unsigned, DataStructures::Table::Row*, _TABLE_BPLUS_TREE_ORDER> *cur = rows.GetListHead();
	DataStructures::Table::Row* row;
	unsigned i;
	if ((unsigned int) databaseTable->SystemAddressColumnIndex==(unsigned int)-1)
		return 0;
	while (cur)
	{
		for (i=0; i < (unsigned)cur->size; i++)
		{
			row = cur->data[i];
			if (RowHasIP(row, systemAddress, databaseTable->SystemAddressColumnIndex ))
			{
				if (rowKey)
					*rowKey=cur->keys[i];
				return row;
			}
		}
		cur=cur->next;
	}
	return 0;
}
bool LightweightDatabaseServer::RowHasIP(DataStructures::Table::Row *row, SystemAddress systemAddress, unsigned SystemAddressColumnIndex)
{
	if ((unsigned int) SystemAddressColumnIndex==(unsigned int)-1)
		return false;

	SystemAddress sysAddr;
	memcpy(&sysAddr, row->cells[SystemAddressColumnIndex]->c, SystemAddress::size());
	return sysAddr==systemAddress;

	// Doesn't work in release for some reason
	//RakAssert(row->cells[SystemAddressColumnIndex]->isEmpty==false);
	//if (memcmp(row->cells[SystemAddressColumnIndex]->c, &systemAddress, SystemAddress::size())==0)
	//	return true;
	// return false;
}
DataStructures::Table::Row * LightweightDatabaseServer::AddRow(LightweightDatabaseServer::DatabaseTable *databaseTable, SystemAddress systemAddress, RakNetGUID guid, bool hasRowId, unsigned rowId)
	{
	// If this is an old row ID (master server crashed) update the ID counter to prevent collisions
	if (rowId >= databaseTable->nextRowId) databaseTable->nextRowId = rowId+1;
	
	DataStructures::Table::Row *row = GetRowFromIP(databaseTable, systemAddress, 0);
	if (databaseTable->oneRowPerSystemAddress && row)
	{
		Log::error_log("This system already has a row\n");
		//return 0; // This system already has a row.
		return row;
	}

	if (databaseTable->autogenerateRowIDs==false)
	{
		// For a new row:
		// rowID required but not specified OR
		// rowId specified but already in the table
		// Then exit
		if (hasRowId==false || databaseTable->table.GetRowByID(rowId))
		{
			Log::error_log("Error: Row ID error while creating row\n");
			return 0;
		}
	}
	else
		rowId=databaseTable->nextRowId++;

	// Add new row
	row = databaseTable->table.AddRow(rowId);

	// Set IP and last update time
	if ( databaseTable->oneRowPerSystemAddress || databaseTable->onlyUpdateOwnRows || databaseTable->removeRowOnPingFailure || databaseTable->removeRowOnDisconnect)
		{
		row->cells[databaseTable->SystemAddressColumnIndex]->Set((char*)&systemAddress, (int) SystemAddress::size());
		row->cells[databaseTable->SystemGuidColumnIndex]->Set((char*)&guid, sizeof(guid));
		}
	if (databaseTable->removeRowOnPingFailure)
		{
		RakNetTime time = RakNet::GetTime();
		row->cells[databaseTable->lastPingResponseColumnIndex]->Set((int) time);
		row->cells[databaseTable->nextPingSendColumnIndex]->Set((int) time+SEND_PING_INTERVAL);
		}

	return row;
	}
void LightweightDatabaseServer::RemoveRowsFromIP(SystemAddress systemAddress)
{
	// Remove rows for tables that do so on a system disconnect
	Log::info_log("Removing rows for IP %s\n",systemAddress.ToString());
	DatabaseTable *databaseTable;
	DataStructures::List<unsigned> removeList;
	DataStructures::Table::Row* row;
	DataStructures::Page<unsigned, DataStructures::Table::Row*, _TABLE_BPLUS_TREE_ORDER> *cur;
	unsigned i,j;
	for (i=0; i < database.Size(); i++)
	{
		databaseTable=database[i];
		if ((unsigned int) databaseTable->SystemAddressColumnIndex!=(unsigned int)-1)
		{
			const DataStructures::BPlusTree<unsigned, DataStructures::Table::Row*, _TABLE_BPLUS_TREE_ORDER> &rows = databaseTable->table.GetRows();
			cur = rows.GetListHead();
			while (cur)
			{
				// Mark dropped entities
				for (j=0; j < (unsigned)cur->size; j++)
				{
					if (RowHasIP(cur->data[j], systemAddress, databaseTable->SystemAddressColumnIndex))
					{
						if (databaseTable->removeRowOnDisconnect)
						{
							Log::debug_log("Found row ID %d\n", cur->keys[j]);
							removeList.Insert(cur->keys[j], __FILE__, __LINE__);
						}
						else if (databaseTable->removeRowOnPingFailure)
						{
							row = cur->data[j];
							row->cells[databaseTable->nextPingSendColumnIndex]->i=(double)(RakNet::GetTime()+SEND_PING_INTERVAL+(randomMT()%1000));
						}
					}
				}
				cur=cur->next;
			}
		}

		for (j=0; j < removeList.Size(); j++)
		{
			addressMap.erase(systemAddress);
			databaseTable->table.RemoveRow(removeList[j]);
			Log::info_log("Force removing row %u\n",removeList[j]);
			Log::debug_log("Current address map count %d\n", addressMap.size());
		}
		removeList.Clear(true, __FILE__,__LINE__);
		
		if (databaseTable->table.GetRowCount() == 0)
		{
			Log::info_log("Deleting unused table for %s\n", databaseTable->tableName);
			RemoveTable(databaseTable->tableName);
		}
	}
}
#ifdef _MSC_VER
#pragma warning( pop )
#endif

#endif // _RAKNET_SUPPORT_*
