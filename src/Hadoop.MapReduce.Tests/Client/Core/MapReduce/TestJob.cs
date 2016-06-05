using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Protocol;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class TestJob
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobToString()
		{
			Cluster cluster = Org.Mockito.Mockito.Mock<Cluster>();
			ClientProtocol client = Org.Mockito.Mockito.Mock<ClientProtocol>();
			Org.Mockito.Mockito.When(cluster.GetClient()).ThenReturn(client);
			JobID jobid = new JobID("1014873536921", 6);
			JobStatus status = new JobStatus(jobid, 0.0f, 0.0f, 0.0f, 0.0f, JobStatus.State.Failed
				, JobPriority.Normal, "root", "TestJobToString", "job file", "tracking url");
			Org.Mockito.Mockito.When(client.GetJobStatus(jobid)).ThenReturn(status);
			Org.Mockito.Mockito.When(client.GetTaskReports(jobid, TaskType.Map)).ThenReturn(new 
				TaskReport[0]);
			Org.Mockito.Mockito.When(client.GetTaskReports(jobid, TaskType.Reduce)).ThenReturn
				(new TaskReport[0]);
			Org.Mockito.Mockito.When(client.GetTaskCompletionEvents(jobid, 0, 10)).ThenReturn
				(new TaskCompletionEvent[0]);
			Job job = Job.GetInstance(cluster, status, new JobConf());
			NUnit.Framework.Assert.IsNotNull(job.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUGICredentialsPropogation()
		{
			Credentials creds = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<object> token = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Security.Token.Token
				>();
			Text tokenService = new Text("service");
			Text secretName = new Text("secret");
			byte[] secret = new byte[] {  };
			creds.AddToken(tokenService, token);
			creds.AddSecretKey(secretName, secret);
			UserGroupInformation.GetLoginUser().AddCredentials(creds);
			JobConf jobConf = new JobConf();
			Job job = new Job(jobConf);
			NUnit.Framework.Assert.AreSame(token, job.GetCredentials().GetToken(tokenService)
				);
			NUnit.Framework.Assert.AreSame(secret, job.GetCredentials().GetSecretKey(secretName
				));
		}
	}
}
