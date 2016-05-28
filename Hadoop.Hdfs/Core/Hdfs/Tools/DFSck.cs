using System;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>
	/// This class provides rudimentary checking of DFS volumes for errors and
	/// sub-optimal conditions.
	/// </summary>
	/// <remarks>
	/// This class provides rudimentary checking of DFS volumes for errors and
	/// sub-optimal conditions.
	/// <p>The tool scans all files and directories, starting from an indicated
	/// root path. The following abnormal conditions are detected and handled:</p>
	/// <ul>
	/// <li>files with blocks that are completely missing from all datanodes.<br/>
	/// In this case the tool can perform one of the following actions:
	/// <ul>
	/// <li>none (
	/// <see cref="org.apache.hadoop.hdfs.server.namenode.NamenodeFsck#FIXING_NONE"/>
	/// )</li>
	/// <li>move corrupted files to /lost+found directory on DFS
	/// (
	/// <see cref="org.apache.hadoop.hdfs.server.namenode.NamenodeFsck#FIXING_MOVE"/>
	/// ). Remaining data blocks are saved as a
	/// block chains, representing longest consecutive series of valid blocks.</li>
	/// <li>delete corrupted files (
	/// <see cref="org.apache.hadoop.hdfs.server.namenode.NamenodeFsck#FIXING_DELETE"/>
	/// )</li>
	/// </ul>
	/// </li>
	/// <li>detect files with under-replicated or over-replicated blocks</li>
	/// </ul>
	/// Additionally, the tool collects a detailed overall DFS statistics, and
	/// optionally can print detailed statistics on block locations and replication
	/// factors of each file.
	/// The tool also provides and option to filter open files during the scan.
	/// </remarks>
	public class DFSck : Configured, Tool
	{
		static DFSck()
		{
			HdfsConfiguration.Init();
		}

		private const string Usage = "Usage: hdfs fsck <path> " + "[-list-corruptfileblocks | "
			 + "[-move | -delete | -openforwrite] " + "[-files [-blocks [-locations | -racks]]]] "
			 + "[-includeSnapshots] " + "[-storagepolicies] [-blockId <blk_Id>]\n" + "\t<path>\tstart checking from this path\n"
			 + "\t-move\tmove corrupted files to /lost+found\n" + "\t-delete\tdelete corrupted files\n"
			 + "\t-files\tprint out files being checked\n" + "\t-openforwrite\tprint out files opened for write\n"
			 + "\t-includeSnapshots\tinclude snapshot data if the given path" + " indicates a snapshottable directory or there are "
			 + "snapshottable directories under it\n" + "\t-list-corruptfileblocks\tprint out list of missing "
			 + "blocks and files they belong to\n" + "\t-blocks\tprint out block report\n" +
			 "\t-locations\tprint out locations for every block\n" + "\t-racks\tprint out network topology for data-node locations\n"
			 + "\t-storagepolicies\tprint out storage policy summary for the blocks\n" + "\t-blockId\tprint out which file this blockId belongs to, locations"
			 + " (nodes, racks) of this block, and other diagnostics info" + " (under replicated, corrupted or not, etc)\n\n"
			 + "Please Note:\n" + "\t1. By default fsck ignores files opened for write, " + 
			"use -openforwrite to report such files. They are usually " + " tagged CORRUPT or HEALTHY depending on their block "
			 + "allocation status\n" + "\t2. Option -includeSnapshots should not be used for comparing stats,"
			 + " should be used only for HEALTH check, as this may contain duplicates" + " if the same file present in both original fs tree "
			 + "and inside snapshots.";

		private readonly UserGroupInformation ugi;

		private readonly TextWriter @out;

		private readonly URLConnectionFactory connectionFactory;

		private readonly bool isSpnegoEnabled;

		/// <summary>Filesystem checker.</summary>
		/// <param name="conf">current Configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public DFSck(Configuration conf)
			: this(conf, System.Console.Out)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public DFSck(Configuration conf, TextWriter @out)
			: base(conf)
		{
			this.ugi = UserGroupInformation.GetCurrentUser();
			this.@out = @out;
			this.connectionFactory = URLConnectionFactory.NewDefaultURLConnectionFactory(conf
				);
			this.isSpnegoEnabled = UserGroupInformation.IsSecurityEnabled();
		}

		/// <summary>Print fsck usage information</summary>
		internal static void PrintUsage(TextWriter @out)
		{
			@out.WriteLine(Usage + "\n");
			ToolRunner.PrintGenericCommandUsage(@out);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Run(string[] args)
		{
			if (args.Length == 0)
			{
				PrintUsage(System.Console.Error);
				return -1;
			}
			try
			{
				return UserGroupInformation.GetCurrentUser().DoAs(new _PrivilegedExceptionAction_149
					(this, args));
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
		}

		private sealed class _PrivilegedExceptionAction_149 : PrivilegedExceptionAction<int
			>
		{
			public _PrivilegedExceptionAction_149(DFSck _enclosing, string[] args)
			{
				this._enclosing = _enclosing;
				this.args = args;
			}

			/// <exception cref="System.Exception"/>
			public int Run()
			{
				return this._enclosing.DoWork(args);
			}

			private readonly DFSck _enclosing;

			private readonly string[] args;
		}

		/*
		* To get the list, we need to call iteratively until the server says
		* there is no more left.
		*/
		/// <exception cref="System.IO.IOException"/>
		private int ListCorruptFileBlocks(string dir, string baseUrl)
		{
			int errCode = -1;
			int numCorrupt = 0;
			int cookie = 0;
			string noCorruptLine = "has no CORRUPT files";
			string noMoreCorruptLine = "has no more CORRUPT files";
			string cookiePrefix = "Cookie:";
			bool allDone = false;
			while (!allDone)
			{
				StringBuilder url = new StringBuilder(baseUrl);
				if (cookie > 0)
				{
					url.Append("&startblockafter=").Append(cookie.ToString());
				}
				Uri path = new Uri(url.ToString());
				URLConnection connection;
				try
				{
					connection = connectionFactory.OpenConnection(path, isSpnegoEnabled);
				}
				catch (AuthenticationException e)
				{
					throw new IOException(e);
				}
				InputStream stream = connection.GetInputStream();
				BufferedReader input = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
				try
				{
					string line = null;
					while ((line = input.ReadLine()) != null)
					{
						if (line.StartsWith(cookiePrefix))
						{
							try
							{
								cookie = System.Convert.ToInt32(line.Split("\t")[1]);
							}
							catch (Exception)
							{
								allDone = true;
								break;
							}
							continue;
						}
						if ((line.EndsWith(noCorruptLine)) || (line.EndsWith(noMoreCorruptLine)) || (line
							.EndsWith(NamenodeFsck.NonexistentStatus)))
						{
							allDone = true;
							break;
						}
						if ((line.IsEmpty()) || (line.StartsWith("FSCK started by")) || (line.StartsWith(
							"The filesystem under path")))
						{
							continue;
						}
						numCorrupt++;
						if (numCorrupt == 1)
						{
							@out.WriteLine("The list of corrupt files under path '" + dir + "' are:");
						}
						@out.WriteLine(line);
					}
				}
				finally
				{
					input.Close();
				}
			}
			@out.WriteLine("The filesystem under path '" + dir + "' has " + numCorrupt + " CORRUPT files"
				);
			if (numCorrupt == 0)
			{
				errCode = 0;
			}
			return errCode;
		}

		/// <exception cref="System.IO.IOException"/>
		private Path GetResolvedPath(string dir)
		{
			Configuration conf = GetConf();
			Path dirPath = new Path(dir);
			FileSystem fs = dirPath.GetFileSystem(conf);
			return fs.ResolvePath(dirPath);
		}

		/// <summary>
		/// Derive the namenode http address from the current file system,
		/// either default or as set by "-fs" in the generic options.
		/// </summary>
		/// <returns>Returns http address or null if failure.</returns>
		/// <exception cref="System.IO.IOException">if we can't determine the active NN address
		/// 	</exception>
		private URI GetCurrentNamenodeAddress(Path target)
		{
			//String nnAddress = null;
			Configuration conf = GetConf();
			//get the filesystem object to verify it is an HDFS system
			FileSystem fs = target.GetFileSystem(conf);
			if (!(fs is DistributedFileSystem))
			{
				System.Console.Error.WriteLine("FileSystem is " + fs.GetUri());
				return null;
			}
			return DFSUtil.GetInfoServer(HAUtil.GetAddressOfActive(fs), conf, DFSUtil.GetHttpClientScheme
				(conf));
		}

		/// <exception cref="System.IO.IOException"/>
		private int DoWork(string[] args)
		{
			StringBuilder url = new StringBuilder();
			url.Append("/fsck?ugi=").Append(ugi.GetShortUserName());
			string dir = null;
			bool doListCorruptFileBlocks = false;
			for (int idx = 0; idx < args.Length; idx++)
			{
				if (args[idx].Equals("-move"))
				{
					url.Append("&move=1");
				}
				else
				{
					if (args[idx].Equals("-delete"))
					{
						url.Append("&delete=1");
					}
					else
					{
						if (args[idx].Equals("-files"))
						{
							url.Append("&files=1");
						}
						else
						{
							if (args[idx].Equals("-openforwrite"))
							{
								url.Append("&openforwrite=1");
							}
							else
							{
								if (args[idx].Equals("-blocks"))
								{
									url.Append("&blocks=1");
								}
								else
								{
									if (args[idx].Equals("-locations"))
									{
										url.Append("&locations=1");
									}
									else
									{
										if (args[idx].Equals("-racks"))
										{
											url.Append("&racks=1");
										}
										else
										{
											if (args[idx].Equals("-storagepolicies"))
											{
												url.Append("&storagepolicies=1");
											}
											else
											{
												if (args[idx].Equals("-list-corruptfileblocks"))
												{
													url.Append("&listcorruptfileblocks=1");
													doListCorruptFileBlocks = true;
												}
												else
												{
													if (args[idx].Equals("-includeSnapshots"))
													{
														url.Append("&includeSnapshots=1");
													}
													else
													{
														if (args[idx].Equals("-blockId"))
														{
															StringBuilder sb = new StringBuilder();
															idx++;
															while (idx < args.Length && !args[idx].StartsWith("-"))
															{
																sb.Append(args[idx]);
																sb.Append(" ");
																idx++;
															}
															url.Append("&blockId=").Append(URLEncoder.Encode(sb.ToString(), "UTF-8"));
														}
														else
														{
															if (!args[idx].StartsWith("-"))
															{
																if (null == dir)
																{
																	dir = args[idx];
																}
																else
																{
																	System.Console.Error.WriteLine("fsck: can only operate on one path at a time '" +
																		 args[idx] + "'");
																	PrintUsage(System.Console.Error);
																	return -1;
																}
															}
															else
															{
																System.Console.Error.WriteLine("fsck: Illegal option '" + args[idx] + "'");
																PrintUsage(System.Console.Error);
																return -1;
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			if (null == dir)
			{
				dir = "/";
			}
			Path dirpath = null;
			URI namenodeAddress = null;
			try
			{
				dirpath = GetResolvedPath(dir);
				namenodeAddress = GetCurrentNamenodeAddress(dirpath);
			}
			catch (IOException ioe)
			{
				System.Console.Error.WriteLine("FileSystem is inaccessible due to:\n" + StringUtils
					.StringifyException(ioe));
			}
			if (namenodeAddress == null)
			{
				//Error message already output in {@link #getCurrentNamenodeAddress()}
				System.Console.Error.WriteLine("DFSck exiting.");
				return 0;
			}
			url.Insert(0, namenodeAddress.ToString());
			url.Append("&path=").Append(URLEncoder.Encode(Path.GetPathWithoutSchemeAndAuthority
				(dirpath).ToString(), "UTF-8"));
			System.Console.Error.WriteLine("Connecting to namenode via " + url.ToString());
			if (doListCorruptFileBlocks)
			{
				return ListCorruptFileBlocks(dir, url.ToString());
			}
			Uri path = new Uri(url.ToString());
			URLConnection connection;
			try
			{
				connection = connectionFactory.OpenConnection(path, isSpnegoEnabled);
			}
			catch (AuthenticationException e)
			{
				throw new IOException(e);
			}
			InputStream stream = connection.GetInputStream();
			BufferedReader input = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
			string line = null;
			string lastLine = null;
			int errCode = -1;
			try
			{
				while ((line = input.ReadLine()) != null)
				{
					@out.WriteLine(line);
					lastLine = line;
				}
			}
			finally
			{
				input.Close();
			}
			if (lastLine.EndsWith(NamenodeFsck.HealthyStatus))
			{
				errCode = 0;
			}
			else
			{
				if (lastLine.EndsWith(NamenodeFsck.CorruptStatus))
				{
					errCode = 1;
				}
				else
				{
					if (lastLine.EndsWith(NamenodeFsck.NonexistentStatus))
					{
						errCode = 0;
					}
					else
					{
						if (lastLine.Contains("Incorrect blockId format:"))
						{
							errCode = 0;
						}
						else
						{
							if (lastLine.EndsWith(NamenodeFsck.DecommissionedStatus))
							{
								errCode = 2;
							}
							else
							{
								if (lastLine.EndsWith(NamenodeFsck.DecommissioningStatus))
								{
									errCode = 3;
								}
							}
						}
					}
				}
			}
			return errCode;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			// -files option is also used by GenericOptionsParser
			// Make sure that is not the first argument for fsck
			int res = -1;
			if ((args.Length == 0) || ("-files".Equals(args[0])))
			{
				PrintUsage(System.Console.Error);
				ToolRunner.PrintGenericCommandUsage(System.Console.Error);
			}
			else
			{
				if (DFSUtil.ParseHelpArgument(args, Usage, System.Console.Out, true))
				{
					res = 0;
				}
				else
				{
					res = ToolRunner.Run(new Org.Apache.Hadoop.Hdfs.Tools.DFSck(new HdfsConfiguration
						()), args);
				}
			}
			System.Environment.Exit(res);
		}
	}
}
