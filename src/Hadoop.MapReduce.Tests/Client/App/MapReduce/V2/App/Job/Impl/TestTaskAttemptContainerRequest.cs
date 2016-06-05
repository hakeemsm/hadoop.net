using System.Collections.Generic;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.Split;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl
{
	public class TestTaskAttemptContainerRequest
	{
		//WARNING: This test must be the only test in this file.  This is because
		// there is an optimization where the credentials passed in are cached
		// statically so they do not need to be recomputed when creating a new
		// ContainerLaunchContext. if other tests run first this code will cache
		// their credentials and this test will fail trying to look for the
		// credentials it inserted in.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAttemptContainerRequest()
		{
			Text SecretKeyAlias = new Text("secretkeyalias");
			byte[] SecretKey = Sharpen.Runtime.GetBytesForString(("secretkey"));
			IDictionary<ApplicationAccessType, string> acls = new Dictionary<ApplicationAccessType
				, string>(1);
			acls[ApplicationAccessType.ViewApp] = "otheruser";
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			JobId jobId = MRBuilderUtils.NewJobId(appId, 1);
			TaskId taskId = MRBuilderUtils.NewTaskId(jobId, 1, TaskType.Map);
			Path jobFile = Org.Mockito.Mockito.Mock<Path>();
			EventHandler eventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			TaskAttemptListener taListener = Org.Mockito.Mockito.Mock<TaskAttemptListener>();
			Org.Mockito.Mockito.When(taListener.GetAddress()).ThenReturn(new IPEndPoint("localhost"
				, 0));
			JobConf jobConf = new JobConf();
			jobConf.SetClass("fs.file.impl", typeof(TestTaskAttemptContainerRequest.StubbedFS
				), typeof(FileSystem));
			jobConf.SetBoolean("fs.file.impl.disable.cache", true);
			jobConf.Set(JobConf.MapredMapTaskEnv, string.Empty);
			// setup UGI for security so tokens and keys are preserved
			jobConf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos"
				);
			UserGroupInformation.SetConfiguration(jobConf);
			Credentials credentials = new Credentials();
			credentials.AddSecretKey(SecretKeyAlias, SecretKey);
			Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken = new Org.Apache.Hadoop.Security.Token.Token
				<JobTokenIdentifier>(Sharpen.Runtime.GetBytesForString(("tokenid")), Sharpen.Runtime.GetBytesForString
				(("tokenpw")), new Text("tokenkind"), new Text("tokenservice"));
			TaskAttemptImpl taImpl = new MapTaskAttemptImpl(taskId, 1, eventHandler, jobFile, 
				1, Org.Mockito.Mockito.Mock<JobSplit.TaskSplitMetaInfo>(), jobConf, taListener, 
				jobToken, credentials, new SystemClock(), null);
			jobConf.Set(MRJobConfig.ApplicationAttemptId, taImpl.GetID().ToString());
			ContainerLaunchContext launchCtx = TaskAttemptImpl.CreateContainerLaunchContext(acls
				, jobConf, jobToken, taImpl.CreateRemoteTask(), TypeConverter.FromYarn(jobId), Org.Mockito.Mockito.Mock
				<WrappedJvmID>(), taListener, credentials);
			NUnit.Framework.Assert.AreEqual("ACLs mismatch", acls, launchCtx.GetApplicationACLs
				());
			Credentials launchCredentials = new Credentials();
			DataInputByteBuffer dibb = new DataInputByteBuffer();
			dibb.Reset(launchCtx.GetTokens());
			launchCredentials.ReadTokenStorageStream(dibb);
			// verify all tokens specified for the task attempt are in the launch context
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in credentials
				.GetAllTokens())
			{
				Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> launchToken = launchCredentials
					.GetToken(token.GetService());
				NUnit.Framework.Assert.IsNotNull("Token " + token.GetService() + " is missing", launchToken
					);
				NUnit.Framework.Assert.AreEqual("Token " + token.GetService() + " mismatch", token
					, launchToken);
			}
			// verify the secret key is in the launch context
			NUnit.Framework.Assert.IsNotNull("Secret key missing", launchCredentials.GetSecretKey
				(SecretKeyAlias));
			NUnit.Framework.Assert.IsTrue("Secret key mismatch", Arrays.Equals(SecretKey, launchCredentials
				.GetSecretKey(SecretKeyAlias)));
		}

		public class StubbedFS : RawLocalFileSystem
		{
			/// <exception cref="System.IO.IOException"/>
			public override FileStatus GetFileStatus(Path f)
			{
				return new FileStatus(1, false, 1, 1, 1, f);
			}
		}
	}
}
