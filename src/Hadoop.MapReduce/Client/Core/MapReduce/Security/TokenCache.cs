using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security
{
	/// <summary>
	/// This class provides user facing APIs for transferring secrets from
	/// the job client to the tasks.
	/// </summary>
	/// <remarks>
	/// This class provides user facing APIs for transferring secrets from
	/// the job client to the tasks.
	/// The secrets can be stored just before submission of jobs and read during
	/// the task execution.
	/// </remarks>
	public class TokenCache
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TokenCache));

		/// <summary>auxiliary method to get user's secret keys..</summary>
		/// <param name="alias"/>
		/// <returns>secret key from the storage</returns>
		public static byte[] GetSecretKey(Credentials credentials, Text alias)
		{
			if (credentials == null)
			{
				return null;
			}
			return credentials.GetSecretKey(alias);
		}

		/// <summary>
		/// Convenience method to obtain delegation tokens from namenodes
		/// corresponding to the paths passed.
		/// </summary>
		/// <param name="credentials"/>
		/// <param name="ps">array of paths</param>
		/// <param name="conf">configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public static void ObtainTokensForNamenodes(Credentials credentials, Path[] ps, Configuration
			 conf)
		{
			if (!UserGroupInformation.IsSecurityEnabled())
			{
				return;
			}
			ObtainTokensForNamenodesInternal(credentials, ps, conf);
		}

		/// <summary>
		/// Remove jobtoken referrals which don't make sense in the context
		/// of the task execution.
		/// </summary>
		/// <param name="conf"/>
		public static void CleanUpTokenReferral(Configuration conf)
		{
			conf.Unset(MRJobConfig.MapreduceJobCredentialsBinary);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void ObtainTokensForNamenodesInternal(Credentials credentials, Path
			[] ps, Configuration conf)
		{
			ICollection<FileSystem> fsSet = new HashSet<FileSystem>();
			foreach (Path p in ps)
			{
				fsSet.AddItem(p.GetFileSystem(conf));
			}
			foreach (FileSystem fs in fsSet)
			{
				ObtainTokensForNamenodesInternal(fs, credentials, conf);
			}
		}

		/// <summary>get delegation token for a specific FS</summary>
		/// <param name="fs"/>
		/// <param name="credentials"/>
		/// <param name="p"/>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		internal static void ObtainTokensForNamenodesInternal(FileSystem fs, Credentials 
			credentials, Configuration conf)
		{
			string delegTokenRenewer = Master.GetMasterPrincipal(conf);
			if (delegTokenRenewer == null || delegTokenRenewer.Length == 0)
			{
				throw new IOException("Can't get Master Kerberos principal for use as renewer");
			}
			MergeBinaryTokens(credentials, conf);
			Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = fs.AddDelegationTokens(
				delegTokenRenewer, credentials);
			if (tokens != null)
			{
				foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in tokens)
				{
					Log.Info("Got dt for " + fs.GetUri() + "; " + token);
				}
			}
		}

		private static void MergeBinaryTokens(Credentials creds, Configuration conf)
		{
			string binaryTokenFilename = conf.Get(MRJobConfig.MapreduceJobCredentialsBinary);
			if (binaryTokenFilename != null)
			{
				Credentials binary;
				try
				{
					binary = Credentials.ReadTokenStorageFile(FileSystem.GetLocal(conf).MakeQualified
						(new Path(binaryTokenFilename)), conf);
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
				// supplement existing tokens with the tokens in the binary file
				creds.MergeAll(binary);
			}
		}

		/// <summary>file name used on HDFS for generated job token</summary>
		[InterfaceAudience.Private]
		public const string JobTokenHdfsFile = "jobToken";

		/// <summary>conf setting for job tokens cache file name</summary>
		[InterfaceAudience.Private]
		public const string JobTokensFilename = "mapreduce.job.jobTokenFile";

		private static readonly Text JobToken = new Text("JobToken");

		private static readonly Text ShuffleToken = new Text("MapReduceShuffleToken");

		private static readonly Text EncSpillKey = new Text("MapReduceEncryptedSpillKey");

		/// <summary>load job token from a file</summary>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Security.Credentials.ReadTokenStorageFile(Org.Apache.Hadoop.FS.Path, Org.Apache.Hadoop.Conf.Configuration) instead, this method is included for compatibility against Hadoop-1."
			)]
		public static Credentials LoadTokens(string jobTokenFile, JobConf conf)
		{
			Path localJobTokenFile = new Path("file:///" + jobTokenFile);
			Credentials ts = Credentials.ReadTokenStorageFile(localJobTokenFile, conf);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Task: Loaded jobTokenFile from: " + localJobTokenFile.ToUri().GetPath(
					) + "; num of sec keys  = " + ts.NumberOfSecretKeys() + " Number of tokens " + ts
					.NumberOfTokens());
			}
			return ts;
		}

		/// <summary>load job token from a file</summary>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Security.Credentials.ReadTokenStorageFile(Org.Apache.Hadoop.FS.Path, Org.Apache.Hadoop.Conf.Configuration) instead, this method is included for compatibility against Hadoop-1."
			)]
		public static Credentials LoadTokens(string jobTokenFile, Configuration conf)
		{
			return LoadTokens(jobTokenFile, new JobConf(conf));
		}

		/// <summary>store job token</summary>
		/// <param name="t"/>
		[InterfaceAudience.Private]
		public static void SetJobToken<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> t
			, Credentials credentials)
			where _T0 : TokenIdentifier
		{
			credentials.AddToken(JobToken, t);
		}

		/// <returns>job token</returns>
		[InterfaceAudience.Private]
		public static Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> GetJobToken
			(Credentials credentials)
		{
			return (Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier>)credentials.GetToken
				(JobToken);
		}

		[InterfaceAudience.Private]
		public static void SetShuffleSecretKey(byte[] key, Credentials credentials)
		{
			credentials.AddSecretKey(ShuffleToken, key);
		}

		[InterfaceAudience.Private]
		public static byte[] GetShuffleSecretKey(Credentials credentials)
		{
			return GetSecretKey(credentials, ShuffleToken);
		}

		[InterfaceAudience.Private]
		public static void SetEncryptedSpillKey(byte[] key, Credentials credentials)
		{
			credentials.AddSecretKey(EncSpillKey, key);
		}

		[InterfaceAudience.Private]
		public static byte[] GetEncryptedSpillKey(Credentials credentials)
		{
			return GetSecretKey(credentials, EncSpillKey);
		}

		/// <param name="namenode"/>
		/// <returns>delegation token</returns>
		[InterfaceAudience.Private]
		[System.ObsoleteAttribute(@"Use Org.Apache.Hadoop.Security.Credentials.GetToken(Org.Apache.Hadoop.IO.Text) instead, this method is included for compatibility against Hadoop-1"
			)]
		public static Org.Apache.Hadoop.Security.Token.Token<object> GetDelegationToken(Credentials
			 credentials, string namenode)
		{
			return (Org.Apache.Hadoop.Security.Token.Token<object>)credentials.GetToken(new Text
				(namenode));
		}
	}
}
