using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.File.Tfile;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	/// <summary>
	/// File system implementation of
	/// <see cref="ApplicationHistoryStore"/>
	/// . In this
	/// implementation, one application will have just one file in the file system,
	/// which contains all the history data of one application, and its attempts and
	/// containers.
	/// <see cref="ApplicationStarted(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationStartData)
	/// 	"/>
	/// is supposed to
	/// be invoked first when writing any history data of one application and it will
	/// open a file, while
	/// <see cref="ApplicationFinished(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Records.ApplicationFinishData)
	/// 	"/>
	/// is
	/// supposed to be last writing operation and will close the file.
	/// </summary>
	public class FileSystemApplicationHistoryStore : AbstractService, ApplicationHistoryStore
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.FileSystemApplicationHistoryStore
			));

		private const string RootDirName = "ApplicationHistoryDataRoot";

		private const int MinBlockSize = 256 * 1024;

		private const string StartDataSuffix = "_start";

		private const string FinishDataSuffix = "_finish";

		private static readonly FsPermission RootDirUmask = FsPermission.CreateImmutable(
			(short)0x1e0);

		private static readonly FsPermission HistoryFileUmask = FsPermission.CreateImmutable
			((short)0x1a0);

		private FileSystem fs;

		private Path rootDirPath;

		private ConcurrentMap<ApplicationId, FileSystemApplicationHistoryStore.HistoryFileWriter
			> outstandingWriters = new ConcurrentHashMap<ApplicationId, FileSystemApplicationHistoryStore.HistoryFileWriter
			>();

		public FileSystemApplicationHistoryStore()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.FileSystemApplicationHistoryStore
				).FullName)
		{
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual FileSystem GetFileSystem(Path path, Configuration conf
			)
		{
			return path.GetFileSystem(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			Configuration conf = GetConfig();
			Path fsWorkingPath = new Path(conf.Get(YarnConfiguration.FsApplicationHistoryStoreUri
				, conf.Get("hadoop.tmp.dir") + "/yarn/timeline/generic-history"));
			rootDirPath = new Path(fsWorkingPath, RootDirName);
			try
			{
				fs = GetFileSystem(fsWorkingPath, conf);
				if (!fs.IsDirectory(rootDirPath))
				{
					fs.Mkdirs(rootDirPath);
					fs.SetPermission(rootDirPath, RootDirUmask);
				}
			}
			catch (IOException e)
			{
				Log.Error("Error when initializing FileSystemHistoryStorage", e);
				throw;
			}
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			try
			{
				foreach (KeyValuePair<ApplicationId, FileSystemApplicationHistoryStore.HistoryFileWriter
					> entry in outstandingWriters)
				{
					entry.Value.Close();
				}
				outstandingWriters.Clear();
			}
			finally
			{
				IOUtils.Cleanup(Log, fs);
			}
			base.ServiceStop();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ApplicationHistoryData GetApplication(ApplicationId appId)
		{
			FileSystemApplicationHistoryStore.HistoryFileReader hfReader = GetHistoryFileReader
				(appId);
			try
			{
				bool readStartData = false;
				bool readFinishData = false;
				ApplicationHistoryData historyData = ApplicationHistoryData.NewInstance(appId, null
					, null, null, null, long.MinValue, long.MinValue, long.MaxValue, null, FinalApplicationStatus
					.Undefined, null);
				while ((!readStartData || !readFinishData) && hfReader.HasNext())
				{
					FileSystemApplicationHistoryStore.HistoryFileReader.Entry entry = hfReader.Next();
					if (entry.key.id.Equals(appId.ToString()))
					{
						if (entry.key.suffix.Equals(StartDataSuffix))
						{
							ApplicationStartData startData = ParseApplicationStartData(entry.value);
							MergeApplicationHistoryData(historyData, startData);
							readStartData = true;
						}
						else
						{
							if (entry.key.suffix.Equals(FinishDataSuffix))
							{
								ApplicationFinishData finishData = ParseApplicationFinishData(entry.value);
								MergeApplicationHistoryData(historyData, finishData);
								readFinishData = true;
							}
						}
					}
				}
				if (!readStartData && !readFinishData)
				{
					return null;
				}
				if (!readStartData)
				{
					Log.Warn("Start information is missing for application " + appId);
				}
				if (!readFinishData)
				{
					Log.Warn("Finish information is missing for application " + appId);
				}
				Log.Info("Completed reading history information of application " + appId);
				return historyData;
			}
			catch (IOException e)
			{
				Log.Error("Error when reading history file of application " + appId, e);
				throw;
			}
			finally
			{
				hfReader.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<ApplicationId, ApplicationHistoryData> GetAllApplications
			()
		{
			IDictionary<ApplicationId, ApplicationHistoryData> historyDataMap = new Dictionary
				<ApplicationId, ApplicationHistoryData>();
			FileStatus[] files = fs.ListStatus(rootDirPath);
			foreach (FileStatus file in files)
			{
				ApplicationId appId = ConverterUtils.ToApplicationId(file.GetPath().GetName());
				try
				{
					ApplicationHistoryData historyData = GetApplication(appId);
					if (historyData != null)
					{
						historyDataMap[appId] = historyData;
					}
				}
				catch (IOException e)
				{
					// Eat the exception not to disturb the getting the next
					// ApplicationHistoryData
					Log.Error("History information of application " + appId + " is not included into the result due to the exception"
						, e);
				}
			}
			return historyDataMap;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<ApplicationAttemptId, ApplicationAttemptHistoryData> GetApplicationAttempts
			(ApplicationId appId)
		{
			IDictionary<ApplicationAttemptId, ApplicationAttemptHistoryData> historyDataMap = 
				new Dictionary<ApplicationAttemptId, ApplicationAttemptHistoryData>();
			FileSystemApplicationHistoryStore.HistoryFileReader hfReader = GetHistoryFileReader
				(appId);
			try
			{
				while (hfReader.HasNext())
				{
					FileSystemApplicationHistoryStore.HistoryFileReader.Entry entry = hfReader.Next();
					if (entry.key.id.StartsWith(ConverterUtils.ApplicationAttemptPrefix))
					{
						ApplicationAttemptId appAttemptId = ConverterUtils.ToApplicationAttemptId(entry.key
							.id);
						if (appAttemptId.GetApplicationId().Equals(appId))
						{
							ApplicationAttemptHistoryData historyData = historyDataMap[appAttemptId];
							if (historyData == null)
							{
								historyData = ApplicationAttemptHistoryData.NewInstance(appAttemptId, null, -1, null
									, null, null, FinalApplicationStatus.Undefined, null);
								historyDataMap[appAttemptId] = historyData;
							}
							if (entry.key.suffix.Equals(StartDataSuffix))
							{
								MergeApplicationAttemptHistoryData(historyData, ParseApplicationAttemptStartData(
									entry.value));
							}
							else
							{
								if (entry.key.suffix.Equals(FinishDataSuffix))
								{
									MergeApplicationAttemptHistoryData(historyData, ParseApplicationAttemptFinishData
										(entry.value));
								}
							}
						}
					}
				}
				Log.Info("Completed reading history information of all application" + " attempts of application "
					 + appId);
			}
			catch (IOException)
			{
				Log.Info("Error when reading history information of some application" + " attempts of application "
					 + appId);
			}
			finally
			{
				hfReader.Close();
			}
			return historyDataMap;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ApplicationAttemptHistoryData GetApplicationAttempt(ApplicationAttemptId
			 appAttemptId)
		{
			FileSystemApplicationHistoryStore.HistoryFileReader hfReader = GetHistoryFileReader
				(appAttemptId.GetApplicationId());
			try
			{
				bool readStartData = false;
				bool readFinishData = false;
				ApplicationAttemptHistoryData historyData = ApplicationAttemptHistoryData.NewInstance
					(appAttemptId, null, -1, null, null, null, FinalApplicationStatus.Undefined, null
					);
				while ((!readStartData || !readFinishData) && hfReader.HasNext())
				{
					FileSystemApplicationHistoryStore.HistoryFileReader.Entry entry = hfReader.Next();
					if (entry.key.id.Equals(appAttemptId.ToString()))
					{
						if (entry.key.suffix.Equals(StartDataSuffix))
						{
							ApplicationAttemptStartData startData = ParseApplicationAttemptStartData(entry.value
								);
							MergeApplicationAttemptHistoryData(historyData, startData);
							readStartData = true;
						}
						else
						{
							if (entry.key.suffix.Equals(FinishDataSuffix))
							{
								ApplicationAttemptFinishData finishData = ParseApplicationAttemptFinishData(entry
									.value);
								MergeApplicationAttemptHistoryData(historyData, finishData);
								readFinishData = true;
							}
						}
					}
				}
				if (!readStartData && !readFinishData)
				{
					return null;
				}
				if (!readStartData)
				{
					Log.Warn("Start information is missing for application attempt " + appAttemptId);
				}
				if (!readFinishData)
				{
					Log.Warn("Finish information is missing for application attempt " + appAttemptId);
				}
				Log.Info("Completed reading history information of application attempt " + appAttemptId
					);
				return historyData;
			}
			catch (IOException e)
			{
				Log.Error("Error when reading history file of application attempt" + appAttemptId
					, e);
				throw;
			}
			finally
			{
				hfReader.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ContainerHistoryData GetContainer(ContainerId containerId)
		{
			FileSystemApplicationHistoryStore.HistoryFileReader hfReader = GetHistoryFileReader
				(containerId.GetApplicationAttemptId().GetApplicationId());
			try
			{
				bool readStartData = false;
				bool readFinishData = false;
				ContainerHistoryData historyData = ContainerHistoryData.NewInstance(containerId, 
					null, null, null, long.MinValue, long.MaxValue, null, int.MaxValue, null);
				while ((!readStartData || !readFinishData) && hfReader.HasNext())
				{
					FileSystemApplicationHistoryStore.HistoryFileReader.Entry entry = hfReader.Next();
					if (entry.key.id.Equals(containerId.ToString()))
					{
						if (entry.key.suffix.Equals(StartDataSuffix))
						{
							ContainerStartData startData = ParseContainerStartData(entry.value);
							MergeContainerHistoryData(historyData, startData);
							readStartData = true;
						}
						else
						{
							if (entry.key.suffix.Equals(FinishDataSuffix))
							{
								ContainerFinishData finishData = ParseContainerFinishData(entry.value);
								MergeContainerHistoryData(historyData, finishData);
								readFinishData = true;
							}
						}
					}
				}
				if (!readStartData && !readFinishData)
				{
					return null;
				}
				if (!readStartData)
				{
					Log.Warn("Start information is missing for container " + containerId);
				}
				if (!readFinishData)
				{
					Log.Warn("Finish information is missing for container " + containerId);
				}
				Log.Info("Completed reading history information of container " + containerId);
				return historyData;
			}
			catch (IOException e)
			{
				Log.Error("Error when reading history file of container " + containerId, e);
				throw;
			}
			finally
			{
				hfReader.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ContainerHistoryData GetAMContainer(ApplicationAttemptId appAttemptId
			)
		{
			ApplicationAttemptHistoryData attemptHistoryData = GetApplicationAttempt(appAttemptId
				);
			if (attemptHistoryData == null || attemptHistoryData.GetMasterContainerId() == null)
			{
				return null;
			}
			return GetContainer(attemptHistoryData.GetMasterContainerId());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IDictionary<ContainerId, ContainerHistoryData> GetContainers(ApplicationAttemptId
			 appAttemptId)
		{
			IDictionary<ContainerId, ContainerHistoryData> historyDataMap = new Dictionary<ContainerId
				, ContainerHistoryData>();
			FileSystemApplicationHistoryStore.HistoryFileReader hfReader = GetHistoryFileReader
				(appAttemptId.GetApplicationId());
			try
			{
				while (hfReader.HasNext())
				{
					FileSystemApplicationHistoryStore.HistoryFileReader.Entry entry = hfReader.Next();
					if (entry.key.id.StartsWith(ConverterUtils.ContainerPrefix))
					{
						ContainerId containerId = ConverterUtils.ToContainerId(entry.key.id);
						if (containerId.GetApplicationAttemptId().Equals(appAttemptId))
						{
							ContainerHistoryData historyData = historyDataMap[containerId];
							if (historyData == null)
							{
								historyData = ContainerHistoryData.NewInstance(containerId, null, null, null, long.MinValue
									, long.MaxValue, null, int.MaxValue, null);
								historyDataMap[containerId] = historyData;
							}
							if (entry.key.suffix.Equals(StartDataSuffix))
							{
								MergeContainerHistoryData(historyData, ParseContainerStartData(entry.value));
							}
							else
							{
								if (entry.key.suffix.Equals(FinishDataSuffix))
								{
									MergeContainerHistoryData(historyData, ParseContainerFinishData(entry.value));
								}
							}
						}
					}
				}
				Log.Info("Completed reading history information of all conatiners" + " of application attempt "
					 + appAttemptId);
			}
			catch (IOException)
			{
				Log.Info("Error when reading history information of some containers" + " of application attempt "
					 + appAttemptId);
			}
			finally
			{
				hfReader.Close();
			}
			return historyDataMap;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplicationStarted(ApplicationStartData appStart)
		{
			FileSystemApplicationHistoryStore.HistoryFileWriter hfWriter = outstandingWriters
				[appStart.GetApplicationId()];
			if (hfWriter == null)
			{
				Path applicationHistoryFile = new Path(rootDirPath, appStart.GetApplicationId().ToString
					());
				try
				{
					hfWriter = new FileSystemApplicationHistoryStore.HistoryFileWriter(this, applicationHistoryFile
						);
					Log.Info("Opened history file of application " + appStart.GetApplicationId());
				}
				catch (IOException e)
				{
					Log.Error("Error when openning history file of application " + appStart.GetApplicationId
						(), e);
					throw;
				}
				outstandingWriters[appStart.GetApplicationId()] = hfWriter;
			}
			else
			{
				throw new IOException("History file of application " + appStart.GetApplicationId(
					) + " is already opened");
			}
			System.Diagnostics.Debug.Assert(appStart is ApplicationStartDataPBImpl);
			try
			{
				hfWriter.WriteHistoryData(new FileSystemApplicationHistoryStore.HistoryDataKey(appStart
					.GetApplicationId().ToString(), StartDataSuffix), ((ApplicationStartDataPBImpl)appStart
					).GetProto().ToByteArray());
				Log.Info("Start information of application " + appStart.GetApplicationId() + " is written"
					);
			}
			catch (IOException e)
			{
				Log.Error("Error when writing start information of application " + appStart.GetApplicationId
					(), e);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplicationFinished(ApplicationFinishData appFinish)
		{
			FileSystemApplicationHistoryStore.HistoryFileWriter hfWriter = GetHistoryFileWriter
				(appFinish.GetApplicationId());
			System.Diagnostics.Debug.Assert(appFinish is ApplicationFinishDataPBImpl);
			try
			{
				hfWriter.WriteHistoryData(new FileSystemApplicationHistoryStore.HistoryDataKey(appFinish
					.GetApplicationId().ToString(), FinishDataSuffix), ((ApplicationFinishDataPBImpl
					)appFinish).GetProto().ToByteArray());
				Log.Info("Finish information of application " + appFinish.GetApplicationId() + " is written"
					);
			}
			catch (IOException e)
			{
				Log.Error("Error when writing finish information of application " + appFinish.GetApplicationId
					(), e);
				throw;
			}
			finally
			{
				hfWriter.Close();
				Sharpen.Collections.Remove(outstandingWriters, appFinish.GetApplicationId());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplicationAttemptStarted(ApplicationAttemptStartData appAttemptStart
			)
		{
			FileSystemApplicationHistoryStore.HistoryFileWriter hfWriter = GetHistoryFileWriter
				(appAttemptStart.GetApplicationAttemptId().GetApplicationId());
			System.Diagnostics.Debug.Assert(appAttemptStart is ApplicationAttemptStartDataPBImpl
				);
			try
			{
				hfWriter.WriteHistoryData(new FileSystemApplicationHistoryStore.HistoryDataKey(appAttemptStart
					.GetApplicationAttemptId().ToString(), StartDataSuffix), ((ApplicationAttemptStartDataPBImpl
					)appAttemptStart).GetProto().ToByteArray());
				Log.Info("Start information of application attempt " + appAttemptStart.GetApplicationAttemptId
					() + " is written");
			}
			catch (IOException e)
			{
				Log.Error("Error when writing start information of application attempt " + appAttemptStart
					.GetApplicationAttemptId(), e);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ApplicationAttemptFinished(ApplicationAttemptFinishData appAttemptFinish
			)
		{
			FileSystemApplicationHistoryStore.HistoryFileWriter hfWriter = GetHistoryFileWriter
				(appAttemptFinish.GetApplicationAttemptId().GetApplicationId());
			System.Diagnostics.Debug.Assert(appAttemptFinish is ApplicationAttemptFinishDataPBImpl
				);
			try
			{
				hfWriter.WriteHistoryData(new FileSystemApplicationHistoryStore.HistoryDataKey(appAttemptFinish
					.GetApplicationAttemptId().ToString(), FinishDataSuffix), ((ApplicationAttemptFinishDataPBImpl
					)appAttemptFinish).GetProto().ToByteArray());
				Log.Info("Finish information of application attempt " + appAttemptFinish.GetApplicationAttemptId
					() + " is written");
			}
			catch (IOException e)
			{
				Log.Error("Error when writing finish information of application attempt " + appAttemptFinish
					.GetApplicationAttemptId(), e);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ContainerStarted(ContainerStartData containerStart)
		{
			FileSystemApplicationHistoryStore.HistoryFileWriter hfWriter = GetHistoryFileWriter
				(containerStart.GetContainerId().GetApplicationAttemptId().GetApplicationId());
			System.Diagnostics.Debug.Assert(containerStart is ContainerStartDataPBImpl);
			try
			{
				hfWriter.WriteHistoryData(new FileSystemApplicationHistoryStore.HistoryDataKey(containerStart
					.GetContainerId().ToString(), StartDataSuffix), ((ContainerStartDataPBImpl)containerStart
					).GetProto().ToByteArray());
				Log.Info("Start information of container " + containerStart.GetContainerId() + " is written"
					);
			}
			catch (IOException e)
			{
				Log.Error("Error when writing start information of container " + containerStart.GetContainerId
					(), e);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ContainerFinished(ContainerFinishData containerFinish)
		{
			FileSystemApplicationHistoryStore.HistoryFileWriter hfWriter = GetHistoryFileWriter
				(containerFinish.GetContainerId().GetApplicationAttemptId().GetApplicationId());
			System.Diagnostics.Debug.Assert(containerFinish is ContainerFinishDataPBImpl);
			try
			{
				hfWriter.WriteHistoryData(new FileSystemApplicationHistoryStore.HistoryDataKey(containerFinish
					.GetContainerId().ToString(), FinishDataSuffix), ((ContainerFinishDataPBImpl)containerFinish
					).GetProto().ToByteArray());
				Log.Info("Finish information of container " + containerFinish.GetContainerId() + 
					" is written");
			}
			catch (IOException e)
			{
				Log.Error("Error when writing finish information of container " + containerFinish
					.GetContainerId(), e);
			}
		}

		/// <exception cref="Com.Google.Protobuf.InvalidProtocolBufferException"/>
		private static ApplicationStartData ParseApplicationStartData(byte[] value)
		{
			return new ApplicationStartDataPBImpl(ApplicationHistoryServerProtos.ApplicationStartDataProto
				.ParseFrom(value));
		}

		/// <exception cref="Com.Google.Protobuf.InvalidProtocolBufferException"/>
		private static ApplicationFinishData ParseApplicationFinishData(byte[] value)
		{
			return new ApplicationFinishDataPBImpl(ApplicationHistoryServerProtos.ApplicationFinishDataProto
				.ParseFrom(value));
		}

		/// <exception cref="Com.Google.Protobuf.InvalidProtocolBufferException"/>
		private static ApplicationAttemptStartData ParseApplicationAttemptStartData(byte[]
			 value)
		{
			return new ApplicationAttemptStartDataPBImpl(ApplicationHistoryServerProtos.ApplicationAttemptStartDataProto
				.ParseFrom(value));
		}

		/// <exception cref="Com.Google.Protobuf.InvalidProtocolBufferException"/>
		private static ApplicationAttemptFinishData ParseApplicationAttemptFinishData(byte
			[] value)
		{
			return new ApplicationAttemptFinishDataPBImpl(ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProto
				.ParseFrom(value));
		}

		/// <exception cref="Com.Google.Protobuf.InvalidProtocolBufferException"/>
		private static ContainerStartData ParseContainerStartData(byte[] value)
		{
			return new ContainerStartDataPBImpl(ApplicationHistoryServerProtos.ContainerStartDataProto
				.ParseFrom(value));
		}

		/// <exception cref="Com.Google.Protobuf.InvalidProtocolBufferException"/>
		private static ContainerFinishData ParseContainerFinishData(byte[] value)
		{
			return new ContainerFinishDataPBImpl(ApplicationHistoryServerProtos.ContainerFinishDataProto
				.ParseFrom(value));
		}

		private static void MergeApplicationHistoryData(ApplicationHistoryData historyData
			, ApplicationStartData startData)
		{
			historyData.SetApplicationName(startData.GetApplicationName());
			historyData.SetApplicationType(startData.GetApplicationType());
			historyData.SetQueue(startData.GetQueue());
			historyData.SetUser(startData.GetUser());
			historyData.SetSubmitTime(startData.GetSubmitTime());
			historyData.SetStartTime(startData.GetStartTime());
		}

		private static void MergeApplicationHistoryData(ApplicationHistoryData historyData
			, ApplicationFinishData finishData)
		{
			historyData.SetFinishTime(finishData.GetFinishTime());
			historyData.SetDiagnosticsInfo(finishData.GetDiagnosticsInfo());
			historyData.SetFinalApplicationStatus(finishData.GetFinalApplicationStatus());
			historyData.SetYarnApplicationState(finishData.GetYarnApplicationState());
		}

		private static void MergeApplicationAttemptHistoryData(ApplicationAttemptHistoryData
			 historyData, ApplicationAttemptStartData startData)
		{
			historyData.SetHost(startData.GetHost());
			historyData.SetRPCPort(startData.GetRPCPort());
			historyData.SetMasterContainerId(startData.GetMasterContainerId());
		}

		private static void MergeApplicationAttemptHistoryData(ApplicationAttemptHistoryData
			 historyData, ApplicationAttemptFinishData finishData)
		{
			historyData.SetDiagnosticsInfo(finishData.GetDiagnosticsInfo());
			historyData.SetTrackingURL(finishData.GetTrackingURL());
			historyData.SetFinalApplicationStatus(finishData.GetFinalApplicationStatus());
			historyData.SetYarnApplicationAttemptState(finishData.GetYarnApplicationAttemptState
				());
		}

		private static void MergeContainerHistoryData(ContainerHistoryData historyData, ContainerStartData
			 startData)
		{
			historyData.SetAllocatedResource(startData.GetAllocatedResource());
			historyData.SetAssignedNode(startData.GetAssignedNode());
			historyData.SetPriority(startData.GetPriority());
			historyData.SetStartTime(startData.GetStartTime());
		}

		private static void MergeContainerHistoryData(ContainerHistoryData historyData, ContainerFinishData
			 finishData)
		{
			historyData.SetFinishTime(finishData.GetFinishTime());
			historyData.SetDiagnosticsInfo(finishData.GetDiagnosticsInfo());
			historyData.SetContainerExitStatus(finishData.GetContainerExitStatus());
			historyData.SetContainerState(finishData.GetContainerState());
		}

		/// <exception cref="System.IO.IOException"/>
		private FileSystemApplicationHistoryStore.HistoryFileWriter GetHistoryFileWriter(
			ApplicationId appId)
		{
			FileSystemApplicationHistoryStore.HistoryFileWriter hfWriter = outstandingWriters
				[appId];
			if (hfWriter == null)
			{
				throw new IOException("History file of application " + appId + " is not opened");
			}
			return hfWriter;
		}

		/// <exception cref="System.IO.IOException"/>
		private FileSystemApplicationHistoryStore.HistoryFileReader GetHistoryFileReader(
			ApplicationId appId)
		{
			Path applicationHistoryFile = new Path(rootDirPath, appId.ToString());
			if (!fs.Exists(applicationHistoryFile))
			{
				throw new IOException("History file for application " + appId + " is not found");
			}
			// The history file is still under writing
			if (outstandingWriters.Contains(appId))
			{
				throw new IOException("History file for application " + appId + " is under writing"
					);
			}
			return new FileSystemApplicationHistoryStore.HistoryFileReader(this, applicationHistoryFile
				);
		}

		private class HistoryFileReader
		{
			private class Entry
			{
				private FileSystemApplicationHistoryStore.HistoryDataKey key;

				private byte[] value;

				public Entry(HistoryFileReader _enclosing, FileSystemApplicationHistoryStore.HistoryDataKey
					 key, byte[] value)
				{
					this._enclosing = _enclosing;
					this.key = key;
					this.value = value;
				}

				private readonly HistoryFileReader _enclosing;
			}

			private TFile.Reader reader;

			private TFile.Reader.Scanner scanner;

			internal FSDataInputStream fsdis;

			/// <exception cref="System.IO.IOException"/>
			public HistoryFileReader(FileSystemApplicationHistoryStore _enclosing, Path historyFile
				)
			{
				this._enclosing = _enclosing;
				this.fsdis = this._enclosing.fs.Open(historyFile);
				this.reader = new TFile.Reader(this.fsdis, this._enclosing.fs.GetFileStatus(historyFile
					).GetLen(), this._enclosing.GetConfig());
				this.Reset();
			}

			public virtual bool HasNext()
			{
				return !this.scanner.AtEnd();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FileSystemApplicationHistoryStore.HistoryFileReader.Entry Next()
			{
				TFile.Reader.Scanner.Entry entry = this.scanner.Entry();
				DataInputStream dis = entry.GetKeyStream();
				FileSystemApplicationHistoryStore.HistoryDataKey key = new FileSystemApplicationHistoryStore.HistoryDataKey
					();
				key.ReadFields(dis);
				dis = entry.GetValueStream();
				byte[] value = new byte[entry.GetValueLength()];
				dis.Read(value);
				this.scanner.Advance();
				return new FileSystemApplicationHistoryStore.HistoryFileReader.Entry(this, key, value
					);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reset()
			{
				IOUtils.Cleanup(FileSystemApplicationHistoryStore.Log, this.scanner);
				this.scanner = this.reader.CreateScanner();
			}

			public virtual void Close()
			{
				IOUtils.Cleanup(FileSystemApplicationHistoryStore.Log, this.scanner, this.reader, 
					this.fsdis);
			}

			private readonly FileSystemApplicationHistoryStore _enclosing;
		}

		private class HistoryFileWriter
		{
			private FSDataOutputStream fsdos;

			private TFile.Writer writer;

			/// <exception cref="System.IO.IOException"/>
			public HistoryFileWriter(FileSystemApplicationHistoryStore _enclosing, Path historyFile
				)
			{
				this._enclosing = _enclosing;
				if (this._enclosing.fs.Exists(historyFile))
				{
					this.fsdos = this._enclosing.fs.Append(historyFile);
				}
				else
				{
					this.fsdos = this._enclosing.fs.Create(historyFile);
				}
				this._enclosing.fs.SetPermission(historyFile, FileSystemApplicationHistoryStore.HistoryFileUmask
					);
				this.writer = new TFile.Writer(this.fsdos, FileSystemApplicationHistoryStore.MinBlockSize
					, this._enclosing.GetConfig().Get(YarnConfiguration.FsApplicationHistoryStoreCompressionType
					, YarnConfiguration.DefaultFsApplicationHistoryStoreCompressionType), null, this
					._enclosing.GetConfig());
			}

			public virtual void Close()
			{
				lock (this)
				{
					IOUtils.Cleanup(FileSystemApplicationHistoryStore.Log, this.writer, this.fsdos);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteHistoryData(FileSystemApplicationHistoryStore.HistoryDataKey
				 key, byte[] value)
			{
				lock (this)
				{
					DataOutputStream dos = null;
					try
					{
						dos = this.writer.PrepareAppendKey(-1);
						key.Write(dos);
					}
					finally
					{
						IOUtils.Cleanup(FileSystemApplicationHistoryStore.Log, dos);
					}
					try
					{
						dos = this.writer.PrepareAppendValue(value.Length);
						dos.Write(value);
					}
					finally
					{
						IOUtils.Cleanup(FileSystemApplicationHistoryStore.Log, dos);
					}
				}
			}

			private readonly FileSystemApplicationHistoryStore _enclosing;
		}

		private class HistoryDataKey : Writable
		{
			private string id;

			private string suffix;

			public HistoryDataKey()
				: this(null, null)
			{
			}

			public HistoryDataKey(string id, string suffix)
			{
				this.id = id;
				this.suffix = suffix;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				@out.WriteUTF(id);
				@out.WriteUTF(suffix);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				id = @in.ReadUTF();
				suffix = @in.ReadUTF();
			}
		}
	}
}
