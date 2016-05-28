using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security
{
	public class TestBinaryTokenFile
	{
		private const string KeySecurityTokenFileName = "key-security-token-file";

		private const string DelegationTokenKey = "Hdfs";

		internal class MySleepMapper : SleepJob.SleepMapper
		{
			// my sleep class
			/// <summary>attempts to access tokenCache as from client</summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(IntWritable key, IntWritable value, Mapper.Context context
				)
			{
				// get context token storage:
				Credentials contextCredentials = context.GetCredentials();
				ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>> contextTokenCollection
					 = contextCredentials.GetAllTokens();
				foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> t in contextTokenCollection)
				{
					System.Console.Out.WriteLine("Context token: [" + t + "]");
				}
				if (contextTokenCollection.Count != 2)
				{
					// one job token and one delegation token
					// fail the test:
					throw new RuntimeException("Exactly 2 tokens are expected in the contextTokenCollection: "
						 + "one job token and one delegation token, but was found " + contextTokenCollection
						.Count + " tokens.");
				}
				Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> dt = contextCredentials.GetToken
					(new Text(DelegationTokenKey));
				if (dt == null)
				{
					throw new RuntimeException("Token for key [" + DelegationTokenKey + "] not found in the job context."
						);
				}
				string tokenFile0 = context.GetConfiguration().Get(MRJobConfig.MapreduceJobCredentialsBinary
					);
				if (tokenFile0 != null)
				{
					throw new RuntimeException("Token file key [" + MRJobConfig.MapreduceJobCredentialsBinary
						 + "] found in the configuration. It should have been removed from the configuration."
						);
				}
				string tokenFile = context.GetConfiguration().Get(KeySecurityTokenFileName);
				if (tokenFile == null)
				{
					throw new RuntimeException("Token file key [" + KeySecurityTokenFileName + "] not found in the job configuration."
						);
				}
				Credentials binaryCredentials = new Credentials();
				binaryCredentials.ReadTokenStorageStream(new DataInputStream(new FileInputStream(
					tokenFile)));
				ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>> binaryTokenCollection
					 = binaryCredentials.GetAllTokens();
				if (binaryTokenCollection.Count != 1)
				{
					throw new RuntimeException("The token collection read from file [" + tokenFile + 
						"] must have size = 1.");
				}
				Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> binTok = binaryTokenCollection
					.GetEnumerator().Next();
				System.Console.Out.WriteLine("The token read from binary file: t = [" + binTok + 
					"]");
				// Verify that dt is same as the token in the file:
				if (!dt.Equals(binTok))
				{
					throw new RuntimeException("Delegation token in job is not same as the token passed in file:"
						 + " tokenInFile=[" + binTok + "], dt=[" + dt + "].");
				}
				// Now test the user tokens.
				UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
				// Print all the UGI tokens for diagnostic purposes:
				ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>> ugiTokenCollection
					 = ugi.GetTokens();
				foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> t_1 in ugiTokenCollection)
				{
					System.Console.Out.WriteLine("UGI token: [" + t_1 + "]");
				}
				Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> ugiToken = ugi.GetCredentials
					().GetToken(new Text(DelegationTokenKey));
				if (ugiToken == null)
				{
					throw new RuntimeException("Token for key [" + DelegationTokenKey + "] not found among the UGI tokens."
						);
				}
				if (!ugiToken.Equals(binTok))
				{
					throw new RuntimeException("UGI token is not same as the token passed in binary file:"
						 + " tokenInBinFile=[" + binTok + "], ugiTok=[" + ugiToken + "].");
				}
				base.Map(key, value, context);
			}
		}

		internal class MySleepJob : SleepJob
		{
			/// <exception cref="System.IO.IOException"/>
			public override Job CreateJob(int numMapper, int numReducer, long mapSleepTime, int
				 mapSleepCount, long reduceSleepTime, int reduceSleepCount)
			{
				Job job = base.CreateJob(numMapper, numReducer, mapSleepTime, mapSleepCount, reduceSleepTime
					, reduceSleepCount);
				job.SetMapperClass(typeof(TestBinaryTokenFile.MySleepMapper));
				//Populate tokens here because security is disabled.
				this.SetupBinaryTokenFile(job);
				return job;
			}

			private void SetupBinaryTokenFile(Job job)
			{
				// Credentials in the job will not have delegation tokens
				// because security is disabled. Fetch delegation tokens
				// and store in binary token file.
				TestBinaryTokenFile.CreateBinaryTokenFile(job.GetConfiguration());
				job.GetConfiguration().Set(MRJobConfig.MapreduceJobCredentialsBinary, TestBinaryTokenFile
					.binaryTokenFileName.ToString());
				// NB: the MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY
				// key now gets deleted from config,
				// so it's not accessible in the job's config. So,
				// we use another key to pass the file name into the job configuration:
				job.GetConfiguration().Set(TestBinaryTokenFile.KeySecurityTokenFileName, TestBinaryTokenFile
					.binaryTokenFileName.ToString());
			}

			internal MySleepJob(TestBinaryTokenFile _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestBinaryTokenFile _enclosing;
		}

		private static MiniMRYarnCluster mrCluster;

		private static MiniDFSCluster dfsCluster;

		private static readonly Path TestDir = new Path(Runtime.GetProperty("test.build.data"
			, "/tmp"));

		private static readonly Path binaryTokenFileName = new Path(TestDir, "tokenFile.binary"
			);

		private const int numSlaves = 1;

		private const int noOfNMs = 1;

		private static Path p1;

		// num of data nodes
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			Configuration conf = new Configuration();
			conf.Set(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
			conf.Set(YarnConfiguration.RmPrincipal, "jt_id/" + SecurityUtil.HostnamePattern +
				 "@APACHE.ORG");
			MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
			builder.CheckExitOnShutdown(true);
			builder.NumDataNodes(numSlaves);
			builder.Format(true);
			builder.Racks(null);
			dfsCluster = builder.Build();
			mrCluster = new MiniMRYarnCluster(typeof(TestBinaryTokenFile).FullName, noOfNMs);
			mrCluster.Init(conf);
			mrCluster.Start();
			NameNodeAdapter.GetDtSecretManager(dfsCluster.GetNamesystem()).StartThreads();
			FileSystem fs = dfsCluster.GetFileSystem();
			p1 = new Path("file1");
			p1 = fs.MakeQualified(p1);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (mrCluster != null)
			{
				mrCluster.Stop();
				mrCluster = null;
			}
			if (dfsCluster != null)
			{
				dfsCluster.Shutdown();
				dfsCluster = null;
			}
		}

		private static void CreateBinaryTokenFile(Configuration conf)
		{
			// Fetch delegation tokens and store in binary token file.
			try
			{
				Credentials cred1 = new Credentials();
				Credentials cred2 = new Credentials();
				TokenCache.ObtainTokensForNamenodesInternal(cred1, new Path[] { p1 }, conf);
				foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> t in cred1.GetAllTokens
					())
				{
					cred2.AddToken(new Text(DelegationTokenKey), t);
				}
				DataOutputStream os = new DataOutputStream(new FileOutputStream(binaryTokenFileName
					.ToString()));
				try
				{
					cred2.WriteTokenStorageToStream(os);
				}
				finally
				{
					os.Close();
				}
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.Fail("Exception " + e);
			}
		}

		/// <summary>run a distributed job and verify that TokenCache is available</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestBinaryTokenFile()
		{
			Configuration conf = mrCluster.GetConfig();
			// provide namenodes names for the job to get the delegation tokens for
			string nnUri = dfsCluster.GetURI(0).ToString();
			conf.Set(MRJobConfig.JobNamenodes, nnUri + "," + nnUri);
			// using argument to pass the file name
			string[] args = new string[] { "-m", "1", "-r", "1", "-mt", "1", "-rt", "1" };
			int res = -1;
			try
			{
				res = ToolRunner.Run(conf, new TestBinaryTokenFile.MySleepJob(this), args);
			}
			catch (Exception e)
			{
				System.Console.Out.WriteLine("Job failed with " + e.GetLocalizedMessage());
				Sharpen.Runtime.PrintStackTrace(e, System.Console.Out);
				NUnit.Framework.Assert.Fail("Job failed");
			}
			NUnit.Framework.Assert.AreEqual("dist job res is not 0:", 0, res);
		}

		/// <summary>
		/// run a distributed job with -tokenCacheFile option parameter and
		/// verify that no exception happens.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenCacheFile()
		{
			Configuration conf = mrCluster.GetConfig();
			CreateBinaryTokenFile(conf);
			// provide namenodes names for the job to get the delegation tokens for
			string nnUri = dfsCluster.GetURI(0).ToString();
			conf.Set(MRJobConfig.JobNamenodes, nnUri + "," + nnUri);
			// using argument to pass the file name
			string[] args = new string[] { "-tokenCacheFile", binaryTokenFileName.ToString(), 
				"-m", "1", "-r", "1", "-mt", "1", "-rt", "1" };
			int res = -1;
			try
			{
				res = ToolRunner.Run(conf, new SleepJob(), args);
			}
			catch (Exception e)
			{
				System.Console.Out.WriteLine("Job failed with " + e.GetLocalizedMessage());
				Sharpen.Runtime.PrintStackTrace(e, System.Console.Out);
				NUnit.Framework.Assert.Fail("Job failed");
			}
			NUnit.Framework.Assert.AreEqual("dist job res is not 0:", 0, res);
		}
	}
}
