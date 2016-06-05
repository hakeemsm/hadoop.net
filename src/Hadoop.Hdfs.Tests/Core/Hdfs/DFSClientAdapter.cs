using System;
using System.IO;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class DFSClientAdapter
	{
		public static DFSClient GetDFSClient(DistributedFileSystem dfs)
		{
			return dfs.dfs;
		}

		public static void SetDFSClient(DistributedFileSystem dfs, DFSClient client)
		{
			dfs.dfs = client;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void StopLeaseRenewer(DistributedFileSystem dfs)
		{
			try
			{
				dfs.dfs.GetLeaseRenewer().InterruptAndJoin();
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static LocatedBlocks CallGetBlockLocations(ClientProtocol namenode, string
			 src, long start, long length)
		{
			return DFSClient.CallGetBlockLocations(namenode, src, start, length);
		}

		/// <exception cref="System.IO.IOException"/>
		public static ClientProtocol GetNamenode(DFSClient client)
		{
			return client.namenode;
		}

		/// <exception cref="System.IO.IOException"/>
		public static DFSClient GetClient(DistributedFileSystem dfs)
		{
			return dfs.dfs;
		}

		public static ExtendedBlock GetPreviousBlock(DFSClient client, long fileId)
		{
			return client.GetPreviousBlock(fileId);
		}

		public static long GetFileId(DFSOutputStream @out)
		{
			return @out.GetFileId();
		}
	}
}
