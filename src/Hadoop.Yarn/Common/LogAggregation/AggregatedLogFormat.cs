using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.IO.Input;
using Org.Apache.Commons.IO.Output;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.File.Tfile;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Logaggregation
{
	public class AggregatedLogFormat
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(AggregatedLogFormat));

		private static readonly AggregatedLogFormat.LogKey ApplicationAclKey = new AggregatedLogFormat.LogKey
			("APPLICATION_ACL");

		private static readonly AggregatedLogFormat.LogKey ApplicationOwnerKey = new AggregatedLogFormat.LogKey
			("APPLICATION_OWNER");

		private static readonly AggregatedLogFormat.LogKey VersionKey = new AggregatedLogFormat.LogKey
			("VERSION");

		private static readonly IDictionary<string, AggregatedLogFormat.LogKey> ReservedKeys;

		private const int Version = 1;

		/// <summary>Umask for the log file.</summary>
		private static readonly FsPermission AppLogFileUmask = FsPermission.CreateImmutable
			((short)(0x1a0 ^ 0x1ff));

		static AggregatedLogFormat()
		{
			//Maybe write out the retention policy.
			//Maybe write out a list of containerLogs skipped by the retention policy.
			ReservedKeys = new Dictionary<string, AggregatedLogFormat.LogKey>();
			ReservedKeys[ApplicationAclKey.ToString()] = ApplicationAclKey;
			ReservedKeys[ApplicationOwnerKey.ToString()] = ApplicationOwnerKey;
			ReservedKeys[VersionKey.ToString()] = VersionKey;
		}

		public class LogKey : Writable
		{
			private string keyString;

			public LogKey()
			{
			}

			public LogKey(ContainerId containerId)
			{
				this.keyString = containerId.ToString();
			}

			public LogKey(string keyString)
			{
				this.keyString = keyString;
			}

			public override int GetHashCode()
			{
				return keyString == null ? 0 : keyString.GetHashCode();
			}

			public override bool Equals(object obj)
			{
				if (obj is AggregatedLogFormat.LogKey)
				{
					AggregatedLogFormat.LogKey other = (AggregatedLogFormat.LogKey)obj;
					if (this.keyString == null)
					{
						return other.keyString == null;
					}
					return this.keyString.Equals(other.keyString);
				}
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			[InterfaceAudience.Private]
			public virtual void Write(DataOutput @out)
			{
				@out.WriteUTF(this.keyString);
			}

			/// <exception cref="System.IO.IOException"/>
			[InterfaceAudience.Private]
			public virtual void ReadFields(DataInput @in)
			{
				this.keyString = @in.ReadUTF();
			}

			public override string ToString()
			{
				return this.keyString;
			}
		}

		public class LogValue
		{
			private readonly IList<string> rootLogDirs;

			private readonly ContainerId containerId;

			private readonly string user;

			private readonly LogAggregationContext logAggregationContext;

			private ICollection<FilePath> uploadedFiles = new HashSet<FilePath>();

			private readonly ICollection<string> alreadyUploadedLogFiles;

			private ICollection<string> allExistingFileMeta = new HashSet<string>();

			private readonly bool appFinished;

			public LogValue(IList<string> rootLogDirs, ContainerId containerId, string user)
				: this(rootLogDirs, containerId, user, null, new HashSet<string>(), true)
			{
			}

			public LogValue(IList<string> rootLogDirs, ContainerId containerId, string user, 
				LogAggregationContext logAggregationContext, ICollection<string> alreadyUploadedLogFiles
				, bool appFinished)
			{
				// TODO Maybe add a version string here. Instead of changing the version of
				// the entire k-v format
				this.rootLogDirs = new AList<string>(rootLogDirs);
				this.containerId = containerId;
				this.user = user;
				// Ensure logs are processed in lexical order
				this.rootLogDirs.Sort();
				this.logAggregationContext = logAggregationContext;
				this.alreadyUploadedLogFiles = alreadyUploadedLogFiles;
				this.appFinished = appFinished;
			}

			private ICollection<FilePath> GetPendingLogFilesToUploadForThisContainer()
			{
				ICollection<FilePath> pendingUploadFiles = new HashSet<FilePath>();
				foreach (string rootLogDir in this.rootLogDirs)
				{
					FilePath appLogDir = new FilePath(rootLogDir, ConverterUtils.ToString(this.containerId
						.GetApplicationAttemptId().GetApplicationId()));
					FilePath containerLogDir = new FilePath(appLogDir, ConverterUtils.ToString(this.containerId
						));
					if (!containerLogDir.IsDirectory())
					{
						continue;
					}
					// ContainerDir may have been deleted by the user.
					Sharpen.Collections.AddAll(pendingUploadFiles, GetPendingLogFilesToUpload(containerLogDir
						));
				}
				return pendingUploadFiles;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutputStream @out, ICollection<FilePath> pendingUploadFiles
				)
			{
				IList<FilePath> fileList = new AList<FilePath>(pendingUploadFiles);
				fileList.Sort();
				foreach (FilePath logFile in fileList)
				{
					// We only aggregate top level files.
					// Ignore anything inside sub-folders.
					if (logFile.IsDirectory())
					{
						Log.Warn(logFile.GetAbsolutePath() + " is a directory. Ignore it.");
						continue;
					}
					FileInputStream @in = null;
					try
					{
						@in = SecureOpenFile(logFile);
					}
					catch (IOException e)
					{
						LogErrorMessage(logFile, e);
						IOUtils.Cleanup(Log, @in);
						continue;
					}
					long fileLength = logFile.Length();
					// Write the logFile Type
					@out.WriteUTF(logFile.GetName());
					// Write the log length as UTF so that it is printable
					@out.WriteUTF(fileLength.ToString());
					// Write the log itself
					try
					{
						byte[] buf = new byte[65535];
						int len = 0;
						long bytesLeft = fileLength;
						while ((len = @in.Read(buf)) != -1)
						{
							//If buffer contents within fileLength, write
							if (len < bytesLeft)
							{
								@out.Write(buf, 0, len);
								bytesLeft -= len;
							}
							else
							{
								//else only write contents within fileLength, then exit early
								@out.Write(buf, 0, (int)bytesLeft);
								break;
							}
						}
						long newLength = logFile.Length();
						if (fileLength < newLength)
						{
							Log.Warn("Aggregated logs truncated by approximately " + (newLength - fileLength)
								 + " bytes.");
						}
						this.uploadedFiles.AddItem(logFile);
					}
					catch (IOException e)
					{
						string message = LogErrorMessage(logFile, e);
						@out.Write(Sharpen.Runtime.GetBytesForString(message, Sharpen.Extensions.GetEncoding
							("UTF-8")));
					}
					finally
					{
						IOUtils.Cleanup(Log, @in);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			[VisibleForTesting]
			public virtual FileInputStream SecureOpenFile(FilePath logFile)
			{
				return SecureIOUtils.OpenForRead(logFile, GetUser(), null);
			}

			private static string LogErrorMessage(FilePath logFile, Exception e)
			{
				string message = "Error aggregating log file. Log file : " + logFile.GetAbsolutePath
					() + ". " + e.Message;
				Log.Error(message, e);
				return message;
			}

			// Added for testing purpose.
			public virtual string GetUser()
			{
				return user;
			}

			private ICollection<FilePath> GetPendingLogFilesToUpload(FilePath containerLogDir
				)
			{
				ICollection<FilePath> candidates = new HashSet<FilePath>(Arrays.AsList(containerLogDir
					.ListFiles()));
				foreach (FilePath logFile in candidates)
				{
					this.allExistingFileMeta.AddItem(GetLogFileMetaData(logFile));
				}
				if (this.logAggregationContext != null && candidates.Count > 0)
				{
					FilterFiles(this.appFinished ? this.logAggregationContext.GetIncludePattern() : this
						.logAggregationContext.GetRolledLogsIncludePattern(), candidates, false);
					FilterFiles(this.appFinished ? this.logAggregationContext.GetExcludePattern() : this
						.logAggregationContext.GetRolledLogsExcludePattern(), candidates, true);
					IEnumerable<FilePath> mask = Iterables.Filter(candidates, new _Predicate_312(this
						));
					candidates = Sets.NewHashSet(mask);
				}
				return candidates;
			}

			private sealed class _Predicate_312 : Predicate<FilePath>
			{
				public _Predicate_312(LogValue _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public bool Apply(FilePath next)
				{
					return !this._enclosing.alreadyUploadedLogFiles.Contains(this._enclosing.GetLogFileMetaData
						(next));
				}

				private readonly LogValue _enclosing;
			}

			private void FilterFiles(string pattern, ICollection<FilePath> candidates, bool exclusion
				)
			{
				if (pattern != null && !pattern.IsEmpty())
				{
					Sharpen.Pattern filterPattern = Sharpen.Pattern.Compile(pattern);
					for (IEnumerator<FilePath> candidatesItr = candidates.GetEnumerator(); candidatesItr
						.HasNext(); )
					{
						FilePath candidate = candidatesItr.Next();
						bool match = filterPattern.Matcher(candidate.GetName()).Find();
						if ((!match && !exclusion) || (match && exclusion))
						{
							candidatesItr.Remove();
						}
					}
				}
			}

			public virtual ICollection<Path> GetCurrentUpLoadedFilesPath()
			{
				ICollection<Path> path = new HashSet<Path>();
				foreach (FilePath file in this.uploadedFiles)
				{
					path.AddItem(new Path(file.GetAbsolutePath()));
				}
				return path;
			}

			public virtual ICollection<string> GetCurrentUpLoadedFileMeta()
			{
				ICollection<string> info = new HashSet<string>();
				foreach (FilePath file in this.uploadedFiles)
				{
					info.AddItem(GetLogFileMetaData(file));
				}
				return info;
			}

			public virtual ICollection<string> GetAllExistingFilesMeta()
			{
				return this.allExistingFileMeta;
			}

			private string GetLogFileMetaData(FilePath file)
			{
				return containerId.ToString() + "_" + file.GetName() + "_" + file.LastModified();
			}
		}

		/// <summary>The writer that writes out the aggregated logs.</summary>
		public class LogWriter
		{
			private readonly FSDataOutputStream fsDataOStream;

			private readonly TFile.Writer writer;

			private FileContext fc;

			/// <exception cref="System.IO.IOException"/>
			public LogWriter(Configuration conf, Path remoteAppLogFile, UserGroupInformation 
				userUgi)
			{
				try
				{
					this.fsDataOStream = userUgi.DoAs(new _PrivilegedExceptionAction_379(this, conf, 
						remoteAppLogFile));
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
				// Keys are not sorted: null arg
				// 256KB minBlockSize : Expected log size for each container too
				this.writer = new TFile.Writer(this.fsDataOStream, 256 * 1024, conf.Get(YarnConfiguration
					.NmLogAggCompressionType, YarnConfiguration.DefaultNmLogAggCompressionType), null
					, conf);
				//Write the version string
				WriteVersion();
			}

			private sealed class _PrivilegedExceptionAction_379 : PrivilegedExceptionAction<FSDataOutputStream
				>
			{
				public _PrivilegedExceptionAction_379(LogWriter _enclosing, Configuration conf, Path
					 remoteAppLogFile)
				{
					this._enclosing = _enclosing;
					this.conf = conf;
					this.remoteAppLogFile = remoteAppLogFile;
				}

				/// <exception cref="System.Exception"/>
				public FSDataOutputStream Run()
				{
					this._enclosing.fc = FileContext.GetFileContext(conf);
					this._enclosing.fc.SetUMask(AggregatedLogFormat.AppLogFileUmask);
					return this._enclosing.fc.Create(remoteAppLogFile, EnumSet.Of(CreateFlag.Create, 
						CreateFlag.Overwrite), new Options.CreateOpts[] {  });
				}

				private readonly LogWriter _enclosing;

				private readonly Configuration conf;

				private readonly Path remoteAppLogFile;
			}

			[VisibleForTesting]
			public virtual TFile.Writer GetWriter()
			{
				return this.writer;
			}

			/// <exception cref="System.IO.IOException"/>
			private void WriteVersion()
			{
				DataOutputStream @out = this.writer.PrepareAppendKey(-1);
				VersionKey.Write(@out);
				@out.Close();
				@out = this.writer.PrepareAppendValue(-1);
				@out.WriteInt(Version);
				@out.Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteApplicationOwner(string user)
			{
				DataOutputStream @out = this.writer.PrepareAppendKey(-1);
				ApplicationOwnerKey.Write(@out);
				@out.Close();
				@out = this.writer.PrepareAppendValue(-1);
				@out.WriteUTF(user);
				@out.Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteApplicationACLs(IDictionary<ApplicationAccessType, string
				> appAcls)
			{
				DataOutputStream @out = this.writer.PrepareAppendKey(-1);
				ApplicationAclKey.Write(@out);
				@out.Close();
				@out = this.writer.PrepareAppendValue(-1);
				foreach (KeyValuePair<ApplicationAccessType, string> entry in appAcls)
				{
					@out.WriteUTF(entry.Key.ToString());
					@out.WriteUTF(entry.Value);
				}
				@out.Close();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Append(AggregatedLogFormat.LogKey logKey, AggregatedLogFormat.LogValue
				 logValue)
			{
				ICollection<FilePath> pendingUploadFiles = logValue.GetPendingLogFilesToUploadForThisContainer
					();
				if (pendingUploadFiles.Count == 0)
				{
					return;
				}
				DataOutputStream @out = this.writer.PrepareAppendKey(-1);
				logKey.Write(@out);
				@out.Close();
				@out = this.writer.PrepareAppendValue(-1);
				logValue.Write(@out, pendingUploadFiles);
				@out.Close();
			}

			public virtual void Close()
			{
				try
				{
					this.writer.Close();
				}
				catch (IOException e)
				{
					Log.Warn("Exception closing writer", e);
				}
				IOUtils.CloseStream(fsDataOStream);
			}
		}

		public class LogReader
		{
			private readonly FSDataInputStream fsDataIStream;

			private readonly TFile.Reader.Scanner scanner;

			private readonly TFile.Reader reader;

			/// <exception cref="System.IO.IOException"/>
			public LogReader(Configuration conf, Path remoteAppLogFile)
			{
				FileContext fileContext = FileContext.GetFileContext(conf);
				this.fsDataIStream = fileContext.Open(remoteAppLogFile);
				reader = new TFile.Reader(this.fsDataIStream, fileContext.GetFileStatus(remoteAppLogFile
					).GetLen(), conf);
				this.scanner = reader.CreateScanner();
			}

			private bool atBeginning = true;

			/// <summary>Returns the owner of the application.</summary>
			/// <returns>the application owner.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual string GetApplicationOwner()
			{
				TFile.Reader.Scanner ownerScanner = reader.CreateScanner();
				AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
				while (!ownerScanner.AtEnd())
				{
					TFile.Reader.Scanner.Entry entry = ownerScanner.Entry();
					key.ReadFields(entry.GetKeyStream());
					if (key.ToString().Equals(ApplicationOwnerKey.ToString()))
					{
						DataInputStream valueStream = entry.GetValueStream();
						return valueStream.ReadUTF();
					}
					ownerScanner.Advance();
				}
				return null;
			}

			/// <summary>Returns ACLs for the application.</summary>
			/// <remarks>
			/// Returns ACLs for the application. An empty map is returned if no ACLs are
			/// found.
			/// </remarks>
			/// <returns>a map of the Application ACLs.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual IDictionary<ApplicationAccessType, string> GetApplicationAcls()
			{
				// TODO Seek directly to the key once a comparator is specified.
				TFile.Reader.Scanner aclScanner = reader.CreateScanner();
				AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
				IDictionary<ApplicationAccessType, string> acls = new Dictionary<ApplicationAccessType
					, string>();
				while (!aclScanner.AtEnd())
				{
					TFile.Reader.Scanner.Entry entry = aclScanner.Entry();
					key.ReadFields(entry.GetKeyStream());
					if (key.ToString().Equals(ApplicationAclKey.ToString()))
					{
						DataInputStream valueStream = entry.GetValueStream();
						while (true)
						{
							string appAccessOp = null;
							string aclString = null;
							try
							{
								appAccessOp = valueStream.ReadUTF();
							}
							catch (EOFException)
							{
								// Valid end of stream.
								break;
							}
							try
							{
								aclString = valueStream.ReadUTF();
							}
							catch (EOFException e)
							{
								throw new YarnRuntimeException("Error reading ACLs", e);
							}
							acls[ApplicationAccessType.ValueOf(appAccessOp)] = aclString;
						}
					}
					aclScanner.Advance();
				}
				return acls;
			}

			/// <summary>Read the next key and return the value-stream.</summary>
			/// <param name="key"/>
			/// <returns>the valueStream if there are more keys or null otherwise.</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual DataInputStream Next(AggregatedLogFormat.LogKey key)
			{
				if (!this.atBeginning)
				{
					this.scanner.Advance();
				}
				else
				{
					this.atBeginning = false;
				}
				if (this.scanner.AtEnd())
				{
					return null;
				}
				TFile.Reader.Scanner.Entry entry = this.scanner.Entry();
				key.ReadFields(entry.GetKeyStream());
				// Skip META keys
				if (ReservedKeys.Contains(key.ToString()))
				{
					return Next(key);
				}
				DataInputStream valueStream = entry.GetValueStream();
				return valueStream;
			}

			/// <summary>
			/// Get a ContainerLogsReader to read the logs for
			/// the specified container.
			/// </summary>
			/// <param name="containerId"/>
			/// <returns>
			/// object to read the container's logs or null if the
			/// logs could not be found
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			[InterfaceAudience.Private]
			public virtual AggregatedLogFormat.ContainerLogsReader GetContainerLogsReader(ContainerId
				 containerId)
			{
				AggregatedLogFormat.ContainerLogsReader logReader = null;
				AggregatedLogFormat.LogKey containerKey = new AggregatedLogFormat.LogKey(containerId
					);
				AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
				DataInputStream valueStream = Next(key);
				while (valueStream != null && !key.Equals(containerKey))
				{
					valueStream = Next(key);
				}
				if (valueStream != null)
				{
					logReader = new AggregatedLogFormat.ContainerLogsReader(valueStream);
				}
				return logReader;
			}

			//TODO  Change Log format and interfaces to be containerId specific.
			// Avoid returning completeValueStreams.
			//    public List<String> getTypesForContainer(DataInputStream valueStream){}
			//    
			//    /**
			//     * @param valueStream
			//     *          The Log stream for the container.
			//     * @param fileType
			//     *          the log type required.
			//     * @return An InputStreamReader for the required log type or null if the
			//     *         type is not found.
			//     * @throws IOException
			//     */
			//    public InputStreamReader getLogStreamForType(DataInputStream valueStream,
			//        String fileType) throws IOException {
			//      valueStream.reset();
			//      try {
			//        while (true) {
			//          String ft = valueStream.readUTF();
			//          String fileLengthStr = valueStream.readUTF();
			//          long fileLength = Long.parseLong(fileLengthStr);
			//          if (ft.equals(fileType)) {
			//            BoundedInputStream bis =
			//                new BoundedInputStream(valueStream, fileLength);
			//            return new InputStreamReader(bis);
			//          } else {
			//            long totalSkipped = 0;
			//            long currSkipped = 0;
			//            while (currSkipped != -1 && totalSkipped < fileLength) {
			//              currSkipped = valueStream.skip(fileLength - totalSkipped);
			//              totalSkipped += currSkipped;
			//            }
			//            // TODO Verify skip behaviour.
			//            if (currSkipped == -1) {
			//              return null;
			//            }
			//          }
			//        }
			//      } catch (EOFException e) {
			//        return null;
			//      }
			//    }
			/// <summary>Writes all logs for a single container to the provided writer.</summary>
			/// <param name="valueStream"/>
			/// <param name="writer"/>
			/// <param name="logUploadedTime"/>
			/// <exception cref="System.IO.IOException"/>
			public static void ReadAcontainerLogs(DataInputStream valueStream, TextWriter writer
				, long logUploadedTime)
			{
				OutputStream os = null;
				TextWriter ps = null;
				try
				{
					os = new WriterOutputStream(writer, Sharpen.Extensions.GetEncoding("UTF-8"));
					ps = new TextWriter(os);
					while (true)
					{
						try
						{
							ReadContainerLogs(valueStream, ps, logUploadedTime);
						}
						catch (EOFException)
						{
							// EndOfFile
							return;
						}
					}
				}
				finally
				{
					IOUtils.Cleanup(Log, ps);
					IOUtils.Cleanup(Log, os);
				}
			}

			/// <summary>Writes all logs for a single container to the provided writer.</summary>
			/// <param name="valueStream"/>
			/// <param name="writer"/>
			/// <exception cref="System.IO.IOException"/>
			public static void ReadAcontainerLogs(DataInputStream valueStream, TextWriter writer
				)
			{
				ReadAcontainerLogs(valueStream, writer, -1);
			}

			/// <exception cref="System.IO.IOException"/>
			private static void ReadContainerLogs(DataInputStream valueStream, TextWriter @out
				, long logUploadedTime)
			{
				byte[] buf = new byte[65535];
				string fileType = valueStream.ReadUTF();
				string fileLengthStr = valueStream.ReadUTF();
				long fileLength = long.Parse(fileLengthStr);
				@out.Write("LogType:");
				@out.WriteLine(fileType);
				if (logUploadedTime != -1)
				{
					@out.Write("Log Upload Time:");
					@out.WriteLine(Times.Format(logUploadedTime));
				}
				@out.Write("LogLength:");
				@out.WriteLine(fileLengthStr);
				@out.WriteLine("Log Contents:");
				long curRead = 0;
				long pendingRead = fileLength - curRead;
				int toRead = pendingRead > buf.Length ? buf.Length : (int)pendingRead;
				int len = valueStream.Read(buf, 0, toRead);
				while (len != -1 && curRead < fileLength)
				{
					@out.Write(buf, 0, len);
					curRead += len;
					pendingRead = fileLength - curRead;
					toRead = pendingRead > buf.Length ? buf.Length : (int)pendingRead;
					len = valueStream.Read(buf, 0, toRead);
				}
				@out.WriteLine("End of LogType:" + fileType);
				@out.WriteLine(string.Empty);
			}

			/// <summary>
			/// Keep calling this till you get a
			/// <see cref="System.IO.EOFException"/>
			/// for getting logs of
			/// all types for a single container.
			/// </summary>
			/// <param name="valueStream"/>
			/// <param name="out"/>
			/// <param name="logUploadedTime"/>
			/// <exception cref="System.IO.IOException"/>
			public static void ReadAContainerLogsForALogType(DataInputStream valueStream, TextWriter
				 @out, long logUploadedTime)
			{
				ReadContainerLogs(valueStream, @out, logUploadedTime);
			}

			/// <summary>
			/// Keep calling this till you get a
			/// <see cref="System.IO.EOFException"/>
			/// for getting logs of
			/// all types for a single container.
			/// </summary>
			/// <param name="valueStream"/>
			/// <param name="out"/>
			/// <exception cref="System.IO.IOException"/>
			public static void ReadAContainerLogsForALogType(DataInputStream valueStream, TextWriter
				 @out)
			{
				ReadAContainerLogsForALogType(valueStream, @out, -1);
			}

			public virtual void Close()
			{
				IOUtils.Cleanup(Log, scanner, reader, fsDataIStream);
			}
		}

		public class ContainerLogsReader
		{
			private DataInputStream valueStream;

			private string currentLogType = null;

			private long currentLogLength = 0;

			private BoundedInputStream currentLogData = null;

			private InputStreamReader currentLogISR;

			public ContainerLogsReader(DataInputStream stream)
			{
				valueStream = stream;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string NextLog()
			{
				if (currentLogData != null && currentLogLength > 0)
				{
					do
					{
						// seek to the end of the current log, relying on BoundedInputStream
						// to prevent seeking past the end of the current log
						if (currentLogData.Skip(currentLogLength) < 0)
						{
							break;
						}
					}
					while (currentLogData.Read() != -1);
				}
				currentLogType = null;
				currentLogLength = 0;
				currentLogData = null;
				currentLogISR = null;
				try
				{
					string logType = valueStream.ReadUTF();
					string logLengthStr = valueStream.ReadUTF();
					currentLogLength = long.Parse(logLengthStr);
					currentLogData = new BoundedInputStream(valueStream, currentLogLength);
					currentLogData.SetPropagateClose(false);
					currentLogISR = new InputStreamReader(currentLogData, Sharpen.Extensions.GetEncoding
						("UTF-8"));
					currentLogType = logType;
				}
				catch (EOFException)
				{
				}
				return currentLogType;
			}

			public virtual string GetCurrentLogType()
			{
				return currentLogType;
			}

			public virtual long GetCurrentLogLength()
			{
				return currentLogLength;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long Skip(long n)
			{
				return currentLogData.Skip(n);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Read()
			{
				return currentLogData.Read();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Read(byte[] buf, int off, int len)
			{
				return currentLogData.Read(buf, off, len);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int Read(char[] buf, int off, int len)
			{
				return currentLogISR.Read(buf, off, len);
			}
		}
	}
}
