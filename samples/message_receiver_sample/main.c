// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

#include <stdlib.h>
#ifdef _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#endif

#include <stdio.h>
#include <stdbool.h>
#include <ctype.h>
#include <math.h>
#include "azure_c_shared_utility/platform.h"
#include "azure_c_shared_utility/tlsio.h"
#include "azure_uamqp_c/message_receiver.h"
#include "azure_uamqp_c/message.h"
#include "azure_uamqp_c/messaging.h"
#include "azure_uamqp_c/amqpalloc.h"
#include "azure_uamqp_c/saslclientio.h"
#include "azure_uamqp_c/sasl_plain.h"

/* This sample connects to an Event Hub, authenticates using SASL PLAIN (key name/key) and then it received all messages for partition 0 */
/* Replace the below settings with your own.*/

#define EH_HOST "<<<Replace with your own EH host (like myeventhub.servicebus.windows.net)>>>"
#define EH_KEY_NAME "<<<Replace with your own key name>>>"
#define EH_KEY "<<<Replace with your own key>>>"
#define EH_NAME "<<<Insert your event hub name here>>>"
#define DEVICE_ID "<<<Insert your device Id here>>>"
#define MAX_PARTITIONS 4

#define MAX_SHORT_VALUE             32767         /* maximum (signed) short value */

unsigned int ConvertToUnsignedInt(const unsigned char data[], int position)
{
	unsigned int result;
	if (data == NULL)
	{
		result = 0;
	}
	else
	{
		result = 0;
		for (int nIndex = 0; nIndex < 4; nIndex++)
		{
			int nTest = data[nIndex + position];
			nTest <<= (nIndex * 8);
			result += nTest;
		}
	}
	return result;
}

static unsigned char* GetByteArray(const char* pszData, size_t* bufferLen)
{
	unsigned char* result;
	if (bufferLen != NULL)
	{
		*bufferLen = strlen(pszData);
		result = (unsigned char*)malloc(*bufferLen);

		size_t nIndex;
		for (nIndex = 0; nIndex < *bufferLen; nIndex++)
		{
			result[nIndex] = (unsigned char)toupper(pszData[nIndex]);
		}
	}
	else
	{
		result = NULL;
	}
	return result;
}

void ComputeHash(const unsigned char pszData[], size_t dataLen, unsigned int nSeed1, unsigned int nSeed2, unsigned int* hash1, unsigned int* hash2)
{
	unsigned int nVal1, nVal2, nVal3;

	nVal1 = nVal2 = nVal3 = (unsigned int)(0xdeadbeef + dataLen + nSeed1);
	nVal3 += nSeed2;

	int nIndex = 0;
	size_t nLen = dataLen;
	while (nLen > 12)
	{
		nVal1 += ConvertToUnsignedInt(pszData, nIndex);
		nVal2 += ConvertToUnsignedInt(pszData, nIndex + 4);
		nVal3 += ConvertToUnsignedInt(pszData, nIndex + 8);

		nVal1 -= nVal3;
		nVal1 ^= (nVal3 << 4) | (nVal3 >> 28);
		nVal3 += nVal2;

		nVal2 -= nVal1;
		nVal2 ^= (nVal1 << 6) | (nVal1 >> 26);
		nVal1 += nVal3;

		nVal3 -= nVal2;
		nVal3 ^= (nVal2 << 8) | (nVal2 >> 24);
		nVal2 += nVal1;

		nVal1 -= nVal3;
		nVal1 ^= (nVal3 << 16) | (nVal3 >> 16);
		nVal3 += nVal2;

		nVal2 -= nVal1;
		nVal2 ^= (nVal1 << 19) | (nVal1 >> 13);
		nVal1 += nVal3;

		nVal3 -= nVal2;
		nVal3 ^= (nVal2 << 4) | (nVal2 >> 28);
		nVal2 += nVal1;

		nIndex += 12;
		nLen -= 12;
	}
	switch (nLen)
	{
	case 12:
		nVal1 += ConvertToUnsignedInt(pszData, nIndex);
		nVal2 += ConvertToUnsignedInt(pszData, nIndex + 4);
		nVal3 += ConvertToUnsignedInt(pszData, nIndex + 8);
		break;
	case 11: // No break;
		nVal3 += ((unsigned int)pszData[nIndex + 10]) << 16;
	case 10: // No break;
		nVal3 += ((unsigned int)pszData[nIndex + 9]) << 8;
	case 9: // No break;
		nVal3 += (unsigned int)pszData[nIndex + 8];
	case 8:
		nVal2 += ConvertToUnsignedInt(pszData, nIndex + 4);
		nVal1 += ConvertToUnsignedInt(pszData, nIndex);
		break;
	case 7:// No break
		nVal2 += ((unsigned int)pszData[nIndex + 6]) << 16;
	case 6:// No break
		nVal2 += ((unsigned int)pszData[nIndex + 5]) << 8;
	case 5:// No break
		nVal2 += ((unsigned int)pszData[nIndex + 4]);
	case 4:
		nVal1 += ConvertToUnsignedInt(pszData, nIndex);
		break;
	case 3:// No break
		nVal1 += ((unsigned int)pszData[nIndex + 2]) << 16;
	case 2:// No break
		nVal1 += ((unsigned int)pszData[nIndex + 1]) << 8;
	case 1:
		nVal1 += (unsigned int)pszData[nIndex];
		break;
	default:
	case 0:
		*hash1 = nVal3;
		*hash2 = nVal2;
		return;
	}

	nVal3 ^= nVal2;
	nVal3 -= (nVal2 << 14) | (nVal2 >> 18);

	nVal1 ^= nVal3;
	nVal1 -= (nVal3 << 11) | (nVal3 >> 21);

	nVal2 ^= nVal1;
	nVal2 -= (nVal1 << 25) | (nVal1 >> 7);

	nVal3 ^= nVal2;
	nVal3 -= (nVal2 << 16) | (nVal2 >> 16);

	nVal1 ^= nVal3;
	nVal1 -= (nVal3 << 4) | (nVal3 >> 28);

	nVal2 ^= nVal1;
	nVal2 -= (nVal1 << 14) | (nVal1 >> 18);

	nVal3 ^= nVal2;
	nVal3 -= (nVal2 << 24) | (nVal2 >> 8);

	*hash1 = nVal3;
	*hash2 = nVal2;
}

static size_t ResolvePartitionIndex(const char* partitionKey, size_t maxPartition)
{
	size_t result;

	// Normalize the Partition Key to be upper case
	size_t len = 0;
	unsigned char* byteArray = GetByteArray(partitionKey, &len);
	if (byteArray == NULL)
	{
		// On failure look at the zero partition
		LogError("Failure Getting Byte Array in ResolvePartitionIndex.");
		result = 0;
	}
	else
	{
		int defaultLogicalPartitionCount = MAX_SHORT_VALUE;
		unsigned int hash1 = 0, hash2 = 0;
		ComputeHash(byteArray, len, 0, 0, &hash1, &hash2);
		unsigned long hashedValue = hash1 ^ hash2;

		// Intended Value truncation from UINT to short
		short sTruncateVal = (short)hashedValue;
		short logicalPartition = (short)abs(sTruncateVal % defaultLogicalPartitionCount);

		double shortRangeWidth = floor((double)defaultLogicalPartitionCount / (double)maxPartition);
		int remainingLogicalPartitions = defaultLogicalPartitionCount - ((int)maxPartition * (int)shortRangeWidth);
		int largeRangeWidth = ((int)shortRangeWidth) + 1;
		int largeRangesLogicalPartitions = largeRangeWidth * remainingLogicalPartitions;
		result = logicalPartition < largeRangesLogicalPartitions ? logicalPartition / largeRangeWidth : remainingLogicalPartitions + ((logicalPartition - largeRangesLogicalPartitions) / (int)shortRangeWidth);

		free(byteArray);
	}
	return result;
}

static AMQP_VALUE on_message_received(const void* context, MESSAGE_HANDLE message)
{
	(void)message;
	(void)context;

	(void)printf("Message received.\r\n");

	return messaging_delivery_accepted();
}

int main(int argc, char** argv)
{
	int result;
    (void)argc, argv;

	XIO_HANDLE sasl_io = NULL;
	CONNECTION_HANDLE connection = NULL;
	SESSION_HANDLE session = NULL;
	LINK_HANDLE link = NULL;
	MESSAGE_RECEIVER_HANDLE message_receiver = NULL;

	amqpalloc_set_memory_tracing_enabled(true);

	if (platform_init() != 0)
	{
		result = -1;
	}
	else
	{
		char partition_str[256];
		size_t last_memory_used = 0;

		/* create SASL plain handler */
		SASL_PLAIN_CONFIG sasl_plain_config = { EH_KEY_NAME, EH_KEY, NULL };
		SASL_MECHANISM_HANDLE sasl_mechanism_handle = saslmechanism_create(saslplain_get_interface(), &sasl_plain_config);

		/* create the TLS IO */
        TLSIO_CONFIG tls_io_config = { EH_HOST, 5671 };
		const IO_INTERFACE_DESCRIPTION* tlsio_interface = platform_get_default_tlsio();
		XIO_HANDLE tls_io = xio_create(tlsio_interface, &tls_io_config);

		/* create the SASL client IO using the TLS IO */
		SASLCLIENTIO_CONFIG sasl_io_config;
        sasl_io_config.underlying_io = tls_io;
        sasl_io_config.sasl_mechanism = sasl_mechanism_handle;
		sasl_io = xio_create(saslclientio_get_interface_description(), &sasl_io_config);

		/* create the connection, session and link */
		connection = connection_create(sasl_io, EH_HOST, "whatever", NULL, NULL);
		connection_set_trace(connection, true);
		session = session_create(connection, NULL, NULL);

		/* set incoming window to 100 for the session */
		session_set_incoming_window(session, 100);

		/* listen only on partition 0 */
		sprintf(partition_str, "amqps://" EH_HOST "/" EH_NAME "/ConsumerGroups/$Default/Partitions/%d", ResolvePartitionIndex(DEVICE_ID, MAX_PARTITIONS));
		AMQP_VALUE source = messaging_create_source(partition_str);
		AMQP_VALUE target = messaging_create_target("ingress-rx");
		link = link_create(session, "receiver-link", role_receiver, source, target);
		link_set_rcv_settle_mode(link, receiver_settle_mode_first);
		amqpvalue_destroy(source);
		amqpvalue_destroy(target);

		/* create a message receiver */
		message_receiver = messagereceiver_create(link, NULL, NULL);
		if ((message_receiver == NULL) ||
			(messagereceiver_open(message_receiver, on_message_received, message_receiver) != 0))
		{
			(void)printf("Cannot open the message receiver.");
			result = -1;
		}
		else
		{
			while (true)
			{
				size_t current_memory_used;
				size_t maximum_memory_used;
				connection_dowork(connection);

				current_memory_used = amqpalloc_get_current_memory_used();
				maximum_memory_used = amqpalloc_get_maximum_memory_used();

				if (current_memory_used != last_memory_used)
				{
					(void)printf("Current memory usage:%lu (max:%lu)\r\n", (unsigned long)current_memory_used, (unsigned long)maximum_memory_used);
					last_memory_used = current_memory_used;
				}
			}

			result = 0;
		}

		messagereceiver_destroy(message_receiver);
		link_destroy(link);
		session_destroy(session);
		connection_destroy(connection);
		platform_deinit();

		(void)printf("Max memory usage:%lu\r\n", (unsigned long)amqpalloc_get_maximum_memory_used());
		(void)printf("Current memory usage:%lu\r\n", (unsigned long)amqpalloc_get_current_memory_used());

#ifdef _CRTDBG_MAP_ALLOC
		_CrtDumpMemoryLeaks();
#endif
	}

	return result;
}
