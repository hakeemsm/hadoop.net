using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Security;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	public class TestKMSAudit
	{
		private TextWriter originalOut;

		private ByteArrayOutputStream memOut;

		private TestKMSAudit.FilterOut filterOut;

		private TextWriter capturedOut;

		private KMSAudit kmsAudit;

		private class FilterOut : FilterOutputStream
		{
			public FilterOut(OutputStream @out)
				: base(@out)
			{
			}

			public virtual void SetOutputStream(OutputStream @out)
			{
				this.@out = @out;
			}
		}

		[SetUp]
		public virtual void SetUp()
		{
			originalOut = System.Console.Error;
			memOut = new ByteArrayOutputStream();
			filterOut = new TestKMSAudit.FilterOut(memOut);
			capturedOut = new TextWriter(filterOut);
			Runtime.SetErr(capturedOut);
			PropertyConfigurator.Configure(Sharpen.Thread.CurrentThread().GetContextClassLoader
				().GetResourceAsStream("log4j-kmsaudit.properties"));
			this.kmsAudit = new KMSAudit(1000);
		}

		[TearDown]
		public virtual void CleanUp()
		{
			Runtime.SetErr(originalOut);
			LogManager.ResetConfiguration();
			kmsAudit.Shutdown();
		}

		private string GetAndResetLogOutput()
		{
			capturedOut.Flush();
			string logOutput = Sharpen.Runtime.GetStringForBytes(memOut.ToByteArray());
			memOut = new ByteArrayOutputStream();
			filterOut.SetOutputStream(memOut);
			return logOutput;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAggregation()
		{
			UserGroupInformation luser = Org.Mockito.Mockito.Mock<UserGroupInformation>();
			Org.Mockito.Mockito.When(luser.GetShortUserName()).ThenReturn("luser");
			kmsAudit.Ok(luser, KMS.KMSOp.DecryptEek, "k1", "testmsg");
			kmsAudit.Ok(luser, KMS.KMSOp.DecryptEek, "k1", "testmsg");
			kmsAudit.Ok(luser, KMS.KMSOp.DecryptEek, "k1", "testmsg");
			kmsAudit.Ok(luser, KMS.KMSOp.DeleteKey, "k1", "testmsg");
			kmsAudit.Ok(luser, KMS.KMSOp.RollNewVersion, "k1", "testmsg");
			kmsAudit.Ok(luser, KMS.KMSOp.DecryptEek, "k1", "testmsg");
			kmsAudit.Ok(luser, KMS.KMSOp.DecryptEek, "k1", "testmsg");
			kmsAudit.Ok(luser, KMS.KMSOp.DecryptEek, "k1", "testmsg");
			Sharpen.Thread.Sleep(1500);
			kmsAudit.Ok(luser, KMS.KMSOp.DecryptEek, "k1", "testmsg");
			Sharpen.Thread.Sleep(1500);
			string @out = GetAndResetLogOutput();
			System.Console.Out.WriteLine(@out);
			Assert.True(@out.Matches("OK\\[op=DECRYPT_EEK, key=k1, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"
				 + "OK\\[op=DELETE_KEY, key=k1, user=luser\\] testmsg" + "OK\\[op=ROLL_NEW_VERSION, key=k1, user=luser\\] testmsg"
				 + "OK\\[op=DECRYPT_EEK, key=k1, user=luser, accessCount=6, interval=[^m]{1,4}ms\\] testmsg"
				 + "OK\\[op=DECRYPT_EEK, key=k1, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"
				));
		}

		// Not aggregated !!
		// Aggregated
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAggregationUnauth()
		{
			UserGroupInformation luser = Org.Mockito.Mockito.Mock<UserGroupInformation>();
			Org.Mockito.Mockito.When(luser.GetShortUserName()).ThenReturn("luser");
			kmsAudit.Unauthorized(luser, KMS.KMSOp.GenerateEek, "k2");
			Sharpen.Thread.Sleep(1000);
			kmsAudit.Ok(luser, KMS.KMSOp.GenerateEek, "k3", "testmsg");
			kmsAudit.Ok(luser, KMS.KMSOp.GenerateEek, "k3", "testmsg");
			kmsAudit.Ok(luser, KMS.KMSOp.GenerateEek, "k3", "testmsg");
			kmsAudit.Ok(luser, KMS.KMSOp.GenerateEek, "k3", "testmsg");
			kmsAudit.Ok(luser, KMS.KMSOp.GenerateEek, "k3", "testmsg");
			kmsAudit.Unauthorized(luser, KMS.KMSOp.GenerateEek, "k3");
			kmsAudit.Ok(luser, KMS.KMSOp.GenerateEek, "k3", "testmsg");
			Sharpen.Thread.Sleep(2000);
			string @out = GetAndResetLogOutput();
			System.Console.Out.WriteLine(@out);
			Assert.True(@out.Matches("UNAUTHORIZED\\[op=GENERATE_EEK, key=k2, user=luser\\] "
				 + "OK\\[op=GENERATE_EEK, key=k3, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"
				 + "OK\\[op=GENERATE_EEK, key=k3, user=luser, accessCount=5, interval=[^m]{1,4}ms\\] testmsg"
				 + "UNAUTHORIZED\\[op=GENERATE_EEK, key=k3, user=luser\\] " + "OK\\[op=GENERATE_EEK, key=k3, user=luser, accessCount=1, interval=[^m]{1,4}ms\\] testmsg"
				));
		}
	}
}
