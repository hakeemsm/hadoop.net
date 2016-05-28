using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// CreateEditsLog
	/// Synopsis: CreateEditsLog -f numFiles StartingBlockId numBlocksPerFile
	/// [-r replicafactor] [-d editsLogDirectory]
	/// Default replication factor is 1
	/// Default edits log directory is /tmp/EditsLogOut
	/// Create a name node's edits log in /tmp/EditsLogOut.
	/// </summary>
	/// <remarks>
	/// CreateEditsLog
	/// Synopsis: CreateEditsLog -f numFiles StartingBlockId numBlocksPerFile
	/// [-r replicafactor] [-d editsLogDirectory]
	/// Default replication factor is 1
	/// Default edits log directory is /tmp/EditsLogOut
	/// Create a name node's edits log in /tmp/EditsLogOut.
	/// The file /tmp/EditsLogOut/current/edits can be copied to a name node's
	/// dfs.namenode.name.dir/current direcotry and the name node can be started as usual.
	/// The files are created in /createdViaInjectingInEditsLog
	/// The file names contain the starting and ending blockIds; hence once can
	/// create multiple edits logs using this command using non overlapping
	/// block ids and feed the files to a single name node.
	/// See Also @link #DataNodeCluster for injecting a set of matching
	/// blocks created with this command into a set of simulated data nodes.
	/// </remarks>
	public class CreateEditsLog
	{
		internal const string BasePath = "/createdViaInjectingInEditsLog";

		internal const string EditsDir = "/tmp/EditsLogOut";

		internal static string edits_dir = EditsDir;

		public const long BlockGenerationStamp = GenerationStamp.LastReservedStamp;

		internal static void AddFiles(FSEditLog editLog, int numFiles, short replication, 
			int blocksPerFile, long startingBlockId, long blockSize, FileNameGenerator nameGenerator
			)
		{
			PermissionStatus p = new PermissionStatus("joeDoe", "people", new FsPermission((short
				)0x1ff));
			INodeId inodeId = new INodeId();
			INodeDirectory dirInode = new INodeDirectory(inodeId.NextValue(), null, p, 0L);
			editLog.LogMkDir(BasePath, dirInode);
			BlockInfoContiguous[] blocks = new BlockInfoContiguous[blocksPerFile];
			for (int iB = 0; iB < blocksPerFile; ++iB)
			{
				blocks[iB] = new BlockInfoContiguous(new Block(0, blockSize, BlockGenerationStamp
					), replication);
			}
			long currentBlockId = startingBlockId;
			long bidAtSync = startingBlockId;
			for (int iF = 0; iF < numFiles; iF++)
			{
				for (int iB_1 = 0; iB_1 < blocksPerFile; ++iB_1)
				{
					blocks[iB_1].SetBlockId(currentBlockId++);
				}
				INodeFile inode = new INodeFile(inodeId.NextValue(), null, p, 0L, 0L, blocks, replication
					, blockSize, unchecked((byte)0));
				inode.ToUnderConstruction(string.Empty, string.Empty);
				// Append path to filename with information about blockIDs 
				string path = "_" + iF + "_B" + blocks[0].GetBlockId() + "_to_B" + blocks[blocksPerFile
					 - 1].GetBlockId() + "_";
				string filePath = nameGenerator.GetNextFileName(string.Empty);
				filePath = filePath + path;
				// Log the new sub directory in edits
				if ((iF % nameGenerator.GetFilesPerDirectory()) == 0)
				{
					string currentDir = nameGenerator.GetCurrentDir();
					dirInode = new INodeDirectory(inodeId.NextValue(), null, p, 0L);
					editLog.LogMkDir(currentDir, dirInode);
				}
				INodeFile fileUc = new INodeFile(inodeId.NextValue(), null, p, 0L, 0L, BlockInfoContiguous
					.EmptyArray, replication, blockSize);
				fileUc.ToUnderConstruction(string.Empty, string.Empty);
				editLog.LogOpenFile(filePath, fileUc, false, false);
				editLog.LogCloseFile(filePath, inode);
				if (currentBlockId - bidAtSync >= 2000)
				{
					// sync every 2K blocks
					editLog.LogSync();
					bidAtSync = currentBlockId;
				}
			}
			System.Console.Out.WriteLine("Created edits log in directory " + edits_dir);
			System.Console.Out.WriteLine(" containing " + numFiles + " File-Creates, each file with "
				 + blocksPerFile + " blocks");
			System.Console.Out.WriteLine(" blocks range: " + startingBlockId + " to " + (currentBlockId
				 - 1));
		}

		internal const string usage = "Usage: createditlogs " + " -f  numFiles startingBlockIds NumBlocksPerFile  [-r replicafactor] "
			 + "[-d editsLogDirectory]\n" + "      Default replication factor is 1\n" + "      Default edits log direcory is "
			 + EditsDir + "\n";

		internal static void PrintUsageExit()
		{
			System.Console.Out.WriteLine(usage);
			System.Environment.Exit(-1);
		}

		internal static void PrintUsageExit(string err)
		{
			System.Console.Out.WriteLine(err);
			PrintUsageExit();
		}

		/// <param name="args">arguments</param>
		/// <exception cref="System.IO.IOException"></exception>
		public static void Main(string[] args)
		{
			long startingBlockId = 1;
			int numFiles = 0;
			short replication = 1;
			int numBlocksPerFile = 0;
			long blockSize = 10;
			if (args.Length == 0)
			{
				PrintUsageExit();
			}
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (args[i].Equals("-h"))
				{
					PrintUsageExit();
				}
				if (args[i].Equals("-f"))
				{
					if (i + 3 >= args.Length || args[i + 1].StartsWith("-") || args[i + 2].StartsWith
						("-") || args[i + 3].StartsWith("-"))
					{
						PrintUsageExit("Missing num files, starting block and/or number of blocks");
					}
					numFiles = System.Convert.ToInt32(args[++i]);
					startingBlockId = System.Convert.ToInt32(args[++i]);
					numBlocksPerFile = System.Convert.ToInt32(args[++i]);
					if (numFiles <= 0 || numBlocksPerFile <= 0)
					{
						PrintUsageExit("numFiles and numBlocksPerFile most be greater than 0");
					}
				}
				else
				{
					if (args[i].Equals("-l"))
					{
						if (i + 1 >= args.Length)
						{
							PrintUsageExit("Missing block length");
						}
						blockSize = long.Parse(args[++i]);
					}
					else
					{
						if (args[i].Equals("-r") || args[i + 1].StartsWith("-"))
						{
							if (i + 1 >= args.Length)
							{
								PrintUsageExit("Missing replication factor");
							}
							replication = short.ParseShort(args[++i]);
						}
						else
						{
							if (args[i].Equals("-d"))
							{
								if (i + 1 >= args.Length || args[i + 1].StartsWith("-"))
								{
									PrintUsageExit("Missing edits logs directory");
								}
								edits_dir = args[++i];
							}
							else
							{
								PrintUsageExit();
							}
						}
					}
				}
			}
			FilePath editsLogDir = new FilePath(edits_dir);
			FilePath subStructureDir = new FilePath(edits_dir + "/" + Storage.StorageDirCurrent
				);
			if (!editsLogDir.Exists())
			{
				if (!editsLogDir.Mkdir())
				{
					System.Console.Out.WriteLine("cannot create " + edits_dir);
					System.Environment.Exit(-1);
				}
			}
			if (!subStructureDir.Exists())
			{
				if (!subStructureDir.Mkdir())
				{
					System.Console.Out.WriteLine("cannot create subdirs of " + edits_dir);
					System.Environment.Exit(-1);
				}
			}
			FileNameGenerator nameGenerator = new FileNameGenerator(BasePath, 100);
			FSEditLog editLog = FSImageTestUtil.CreateStandaloneEditLog(editsLogDir);
			editLog.OpenForWrite();
			AddFiles(editLog, numFiles, replication, numBlocksPerFile, startingBlockId, blockSize
				, nameGenerator);
			editLog.LogSync();
			editLog.Close();
		}
	}
}
