using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Org.Apache.Hadoop.Mapreduce.Protocol;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Codehaus.Jackson;
using Org.Codehaus.Jackson.Map;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	internal class JobSubmitter
	{
		protected internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.JobSubmitter
			));

		private const string ShuffleKeygenAlgorithm = "HmacSHA1";

		private const int ShuffleKeyLength = 64;

		private FileSystem jtFs;

		private ClientProtocol submitClient;

		private string submitHostName;

		private string submitHostAddress;

		/// <exception cref="System.IO.IOException"/>
		internal JobSubmitter(FileSystem submitFs, ClientProtocol submitClient)
		{
			this.submitClient = submitClient;
			this.jtFs = submitFs;
		}

		/// <summary>
		/// configure the jobconf of the user with the command line options of
		/// -libjars, -files, -archives.
		/// </summary>
		/// <param name="job"/>
		/// <exception cref="System.IO.IOException"/>
		private void CopyAndConfigureFiles(Job job, Path jobSubmitDir)
		{
			JobResourceUploader rUploader = new JobResourceUploader(jtFs);
			rUploader.UploadFiles(job, jobSubmitDir);
			// Get the working directory. If not set, sets it to filesystem working dir
			// This code has been added so that working directory reset before running
			// the job. This is necessary for backward compatibility as other systems
			// might use the public API JobConf#setWorkingDirectory to reset the working
			// directory.
			job.GetWorkingDirectory();
		}

		/// <summary>Internal method for submitting jobs to the system.</summary>
		/// <remarks>
		/// Internal method for submitting jobs to the system.
		/// <p>The job submission process involves:
		/// <ol>
		/// <li>
		/// Checking the input and output specifications of the job.
		/// </li>
		/// <li>
		/// Computing the
		/// <see cref="InputSplit"/>
		/// s for the job.
		/// </li>
		/// <li>
		/// Setup the requisite accounting information for the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Filecache.DistributedCache"/>
		/// of the job, if necessary.
		/// </li>
		/// <li>
		/// Copying the job's jar and configuration to the map-reduce system
		/// directory on the distributed file-system.
		/// </li>
		/// <li>
		/// Submitting the job to the <code>JobTracker</code> and optionally
		/// monitoring it's status.
		/// </li>
		/// </ol></p>
		/// </remarks>
		/// <param name="job">the configuration to submit</param>
		/// <param name="cluster">the handle to the Cluster</param>
		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual JobStatus SubmitJobInternal(Job job, Cluster cluster)
		{
			//validate the jobs output specs 
			CheckSpecs(job);
			Configuration conf = job.GetConfiguration();
			AddMRFrameworkToDistributedCache(conf);
			Path jobStagingArea = JobSubmissionFiles.GetStagingDir(cluster, conf);
			//configure the command line options correctly on the submitting dfs
			IPAddress ip = Sharpen.Runtime.GetLocalHost();
			if (ip != null)
			{
				submitHostAddress = ip.GetHostAddress();
				submitHostName = ip.GetHostName();
				conf.Set(MRJobConfig.JobSubmithost, submitHostName);
				conf.Set(MRJobConfig.JobSubmithostaddr, submitHostAddress);
			}
			JobID jobId = submitClient.GetNewJobID();
			job.SetJobID(jobId);
			Path submitJobDir = new Path(jobStagingArea, jobId.ToString());
			JobStatus status = null;
			try
			{
				conf.Set(MRJobConfig.UserName, UserGroupInformation.GetCurrentUser().GetShortUserName
					());
				conf.Set("hadoop.http.filter.initializers", "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer"
					);
				conf.Set(MRJobConfig.MapreduceJobDir, submitJobDir.ToString());
				Log.Debug("Configuring job " + jobId + " with " + submitJobDir + " as the submit dir"
					);
				// get delegation token for the dir
				TokenCache.ObtainTokensForNamenodes(job.GetCredentials(), new Path[] { submitJobDir
					 }, conf);
				PopulateTokenCache(conf, job.GetCredentials());
				// generate a secret to authenticate shuffle transfers
				if (TokenCache.GetShuffleSecretKey(job.GetCredentials()) == null)
				{
					KeyGenerator keyGen;
					try
					{
						keyGen = KeyGenerator.GetInstance(ShuffleKeygenAlgorithm);
						keyGen.Init(ShuffleKeyLength);
					}
					catch (NoSuchAlgorithmException e)
					{
						throw new IOException("Error generating shuffle secret key", e);
					}
					SecretKey shuffleKey = keyGen.GenerateKey();
					TokenCache.SetShuffleSecretKey(shuffleKey.GetEncoded(), job.GetCredentials());
				}
				if (CryptoUtils.IsEncryptedSpillEnabled(conf))
				{
					conf.SetInt(MRJobConfig.MrAmMaxAttempts, 1);
					Log.Warn("Max job attempts set to 1 since encrypted intermediate" + "data spill is enabled"
						);
				}
				CopyAndConfigureFiles(job, submitJobDir);
				Path submitJobFile = JobSubmissionFiles.GetJobConfPath(submitJobDir);
				// Create the splits for the job
				Log.Debug("Creating splits at " + jtFs.MakeQualified(submitJobDir));
				int maps = WriteSplits(job, submitJobDir);
				conf.SetInt(MRJobConfig.NumMaps, maps);
				Log.Info("number of splits:" + maps);
				// write "queue admins of the queue to which job is being submitted"
				// to job file.
				string queue = conf.Get(MRJobConfig.QueueName, JobConf.DefaultQueueName);
				AccessControlList acl = submitClient.GetQueueAdmins(queue);
				conf.Set(QueueManager.ToFullPropertyName(queue, QueueACL.AdministerJobs.GetAclName
					()), acl.GetAclString());
				// removing jobtoken referrals before copying the jobconf to HDFS
				// as the tasks don't need this setting, actually they may break
				// because of it if present as the referral will point to a
				// different job.
				TokenCache.CleanUpTokenReferral(conf);
				if (conf.GetBoolean(MRJobConfig.JobTokenTrackingIdsEnabled, MRJobConfig.DefaultJobTokenTrackingIdsEnabled
					))
				{
					// Add HDFS tracking ids
					AList<string> trackingIds = new AList<string>();
					foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> t in job.GetCredentials
						().GetAllTokens())
					{
						trackingIds.AddItem(t.DecodeIdentifier().GetTrackingId());
					}
					conf.SetStrings(MRJobConfig.JobTokenTrackingIds, Sharpen.Collections.ToArray(trackingIds
						, new string[trackingIds.Count]));
				}
				// Set reservation info if it exists
				ReservationId reservationId = job.GetReservationId();
				if (reservationId != null)
				{
					conf.Set(MRJobConfig.ReservationId, reservationId.ToString());
				}
				// Write job file to submit dir
				WriteConf(conf, submitJobFile);
				//
				// Now, actually submit the job (using the submit name)
				//
				PrintTokens(jobId, job.GetCredentials());
				status = submitClient.SubmitJob(jobId, submitJobDir.ToString(), job.GetCredentials
					());
				if (status != null)
				{
					return status;
				}
				else
				{
					throw new IOException("Could not launch job");
				}
			}
			finally
			{
				if (status == null)
				{
					Log.Info("Cleaning up the staging area " + submitJobDir);
					if (jtFs != null && submitJobDir != null)
					{
						jtFs.Delete(submitJobDir, true);
					}
				}
			}
		}

		/// <exception cref="System.TypeLoadException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		private void CheckSpecs(Job job)
		{
			JobConf jConf = (JobConf)job.GetConfiguration();
			// Check the output specification
			if (jConf.GetNumReduceTasks() == 0 ? jConf.GetUseNewMapper() : jConf.GetUseNewReducer
				())
			{
				OutputFormat<object, object> output = ReflectionUtils.NewInstance(job.GetOutputFormatClass
					(), job.GetConfiguration());
				output.CheckOutputSpecs(job);
			}
			else
			{
				jConf.GetOutputFormat().CheckOutputSpecs(jtFs, jConf);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteConf(Configuration conf, Path jobFile)
		{
			// Write job file to JobTracker's fs        
			FSDataOutputStream @out = FileSystem.Create(jtFs, jobFile, new FsPermission(JobSubmissionFiles
				.JobFilePermission));
			try
			{
				conf.WriteXml(@out);
			}
			finally
			{
				@out.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void PrintTokens(JobID jobId, Credentials credentials)
		{
			Log.Info("Submitting tokens for job: " + jobId);
			foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in credentials.GetAllTokens
				())
			{
				Log.Info(token);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		private int WriteNewSplits<T>(JobContext job, Path jobSubmitDir)
			where T : InputSplit
		{
			Configuration conf = job.GetConfiguration();
			InputFormat<object, object> input = ReflectionUtils.NewInstance(job.GetInputFormatClass
				(), conf);
			IList<InputSplit> splits = input.GetSplits(job);
			T[] array = (T[])Sharpen.Collections.ToArray(splits, new InputSplit[splits.Count]
				);
			// sort the splits into order based on size, so that the biggest
			// go first
			Arrays.Sort(array, new JobSubmitter.SplitComparator());
			JobSplitWriter.CreateSplitFiles(jobSubmitDir, conf, jobSubmitDir.GetFileSystem(conf
				), array);
			return array.Length;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		private int WriteSplits(JobContext job, Path jobSubmitDir)
		{
			JobConf jConf = (JobConf)job.GetConfiguration();
			int maps;
			if (jConf.GetUseNewMapper())
			{
				maps = WriteNewSplits(job, jobSubmitDir);
			}
			else
			{
				maps = WriteOldSplits(jConf, jobSubmitDir);
			}
			return maps;
		}

		//method to write splits for old api mapper.
		/// <exception cref="System.IO.IOException"/>
		private int WriteOldSplits(JobConf job, Path jobSubmitDir)
		{
			InputSplit[] splits = job.GetInputFormat().GetSplits(job, job.GetNumMapTasks());
			// sort the splits into order based on size, so that the biggest
			// go first
			Arrays.Sort(splits, new _IComparer_332());
			JobSplitWriter.CreateSplitFiles(jobSubmitDir, job, jobSubmitDir.GetFileSystem(job
				), splits);
			return splits.Length;
		}

		private sealed class _IComparer_332 : IComparer<InputSplit>
		{
			public _IComparer_332()
			{
			}

			public int Compare(InputSplit a, InputSplit b)
			{
				try
				{
					long left = a.GetLength();
					long right = b.GetLength();
					if (left == right)
					{
						return 0;
					}
					else
					{
						if (left < right)
						{
							return 1;
						}
						else
						{
							return -1;
						}
					}
				}
				catch (IOException ie)
				{
					throw new RuntimeException("Problem getting input split size", ie);
				}
			}
		}

		private class SplitComparator : IComparer<InputSplit>
		{
			public virtual int Compare(InputSplit o1, InputSplit o2)
			{
				try
				{
					long len1 = o1.GetLength();
					long len2 = o2.GetLength();
					if (len1 < len2)
					{
						return 1;
					}
					else
					{
						if (len1 == len2)
						{
							return 0;
						}
						else
						{
							return -1;
						}
					}
				}
				catch (IOException ie)
				{
					throw new RuntimeException("exception in compare", ie);
				}
				catch (Exception ie)
				{
					throw new RuntimeException("exception in compare", ie);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadTokensFromFiles(Configuration conf, Credentials credentials)
		{
			// add tokens and secrets coming from a token storage file
			string binaryTokenFilename = conf.Get("mapreduce.job.credentials.binary");
			if (binaryTokenFilename != null)
			{
				Credentials binary = Credentials.ReadTokenStorageFile(FileSystem.GetLocal(conf).MakeQualified
					(new Path(binaryTokenFilename)), conf);
				credentials.AddAll(binary);
			}
			// add secret keys coming from a json file
			string tokensFileName = conf.Get("mapreduce.job.credentials.json");
			if (tokensFileName != null)
			{
				Log.Info("loading user's secret keys from " + tokensFileName);
				string localFileName = new Path(tokensFileName).ToUri().GetPath();
				bool json_error = false;
				try
				{
					// read JSON
					ObjectMapper mapper = new ObjectMapper();
					IDictionary<string, string> nm = mapper.ReadValue<IDictionary>(new FilePath(localFileName
						));
					foreach (KeyValuePair<string, string> ent in nm)
					{
						credentials.AddSecretKey(new Text(ent.Key), Sharpen.Runtime.GetBytesForString(ent
							.Value, Charsets.Utf8));
					}
				}
				catch (JsonMappingException)
				{
					json_error = true;
				}
				catch (JsonParseException)
				{
					json_error = true;
				}
				if (json_error)
				{
					Log.Warn("couldn't parse Token Cache JSON file with user secret keys");
				}
			}
		}

		//get secret keys and tokens and store them into TokenCache
		/// <exception cref="System.IO.IOException"/>
		private void PopulateTokenCache(Configuration conf, Credentials credentials)
		{
			ReadTokensFromFiles(conf, credentials);
			// add the delegation tokens from configuration
			string[] nameNodes = conf.GetStrings(MRJobConfig.JobNamenodes);
			Log.Debug("adding the following namenodes' delegation tokens:" + Arrays.ToString(
				nameNodes));
			if (nameNodes != null)
			{
				Path[] ps = new Path[nameNodes.Length];
				for (int i = 0; i < nameNodes.Length; i++)
				{
					ps[i] = new Path(nameNodes[i]);
				}
				TokenCache.ObtainTokensForNamenodes(credentials, ps, conf);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void AddMRFrameworkToDistributedCache(Configuration conf)
		{
			string framework = conf.Get(MRJobConfig.MapreduceApplicationFrameworkPath, string.Empty
				);
			if (!framework.IsEmpty())
			{
				URI uri;
				try
				{
					uri = new URI(framework);
				}
				catch (URISyntaxException e)
				{
					throw new ArgumentException("Unable to parse '" + framework + "' as a URI, check the setting for "
						 + MRJobConfig.MapreduceApplicationFrameworkPath, e);
				}
				string linkedName = uri.GetFragment();
				// resolve any symlinks in the URI path so using a "current" symlink
				// to point to a specific version shows the specific version
				// in the distributed cache configuration
				FileSystem fs = FileSystem.Get(conf);
				Path frameworkPath = fs.MakeQualified(new Path(uri.GetScheme(), uri.GetAuthority(
					), uri.GetPath()));
				FileContext fc = FileContext.GetFileContext(frameworkPath.ToUri(), conf);
				frameworkPath = fc.ResolvePath(frameworkPath);
				uri = frameworkPath.ToUri();
				try
				{
					uri = new URI(uri.GetScheme(), uri.GetAuthority(), uri.GetPath(), null, linkedName
						);
				}
				catch (URISyntaxException e)
				{
					throw new ArgumentException(e);
				}
				DistributedCache.AddCacheArchive(uri, conf);
			}
		}
	}
}
