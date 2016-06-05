using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;


namespace Org.Apache.Hadoop.Security.Alias
{
	public class TestCredShell
	{
		private readonly ByteArrayOutputStream outContent = new ByteArrayOutputStream();

		private readonly ByteArrayOutputStream errContent = new ByteArrayOutputStream();

		private static readonly FilePath tmpDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp"), "creds");

		private string jceksProvider;

		/* The default JCEKS provider - for testing purposes */
		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			Runtime.SetOut(new TextWriter(outContent));
			Runtime.SetErr(new TextWriter(errContent));
			Path jksPath = new Path(tmpDir.ToString(), "keystore.jceks");
			new FilePath(jksPath.ToString()).Delete();
			jceksProvider = "jceks://file" + jksPath.ToUri();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCredentialSuccessfulLifecycle()
		{
			outContent.Reset();
			string[] args1 = new string[] { "create", "credential1", "-value", "p@ssw0rd", "-provider"
				, jceksProvider };
			int rc = 0;
			CredentialShell cs = new CredentialShell();
			cs.SetConf(new Configuration());
			rc = cs.Run(args1);
			Assert.Equal(outContent.ToString(), 0, rc);
			Assert.True(outContent.ToString().Contains("credential1 has been successfully "
				 + "created."));
			outContent.Reset();
			string[] args2 = new string[] { "list", "-provider", jceksProvider };
			rc = cs.Run(args2);
			Assert.Equal(0, rc);
			Assert.True(outContent.ToString().Contains("credential1"));
			outContent.Reset();
			string[] args4 = new string[] { "delete", "credential1", "-f", "-provider", jceksProvider
				 };
			rc = cs.Run(args4);
			Assert.Equal(0, rc);
			Assert.True(outContent.ToString().Contains("credential1 has been successfully "
				 + "deleted."));
			outContent.Reset();
			string[] args5 = new string[] { "list", "-provider", jceksProvider };
			rc = cs.Run(args5);
			Assert.Equal(0, rc);
			NUnit.Framework.Assert.IsFalse(outContent.ToString(), outContent.ToString().Contains
				("credential1"));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInvalidProvider()
		{
			string[] args1 = new string[] { "create", "credential1", "-value", "p@ssw0rd", "-provider"
				, "sdff://file/tmp/credstore.jceks" };
			int rc = 0;
			CredentialShell cs = new CredentialShell();
			cs.SetConf(new Configuration());
			rc = cs.Run(args1);
			Assert.Equal(1, rc);
			Assert.True(outContent.ToString().Contains("There are no valid "
				 + "CredentialProviders configured."));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestTransientProviderWarning()
		{
			string[] args1 = new string[] { "create", "credential1", "-value", "p@ssw0rd", "-provider"
				, "user:///" };
			int rc = 0;
			CredentialShell cs = new CredentialShell();
			cs.SetConf(new Configuration());
			rc = cs.Run(args1);
			Assert.Equal(outContent.ToString(), 0, rc);
			Assert.True(outContent.ToString().Contains("WARNING: you are modifying a "
				 + "transient provider."));
			string[] args2 = new string[] { "delete", "credential1", "-f", "-provider", "user:///"
				 };
			rc = cs.Run(args2);
			Assert.Equal(outContent.ToString(), 0, rc);
			Assert.True(outContent.ToString().Contains("credential1 has been successfully "
				 + "deleted."));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestTransientProviderOnlyConfig()
		{
			string[] args1 = new string[] { "create", "credential1" };
			int rc = 0;
			CredentialShell cs = new CredentialShell();
			Configuration config = new Configuration();
			config.Set(CredentialProviderFactory.CredentialProviderPath, "user:///");
			cs.SetConf(config);
			rc = cs.Run(args1);
			Assert.Equal(1, rc);
			Assert.True(outContent.ToString().Contains("There are no valid "
				 + "CredentialProviders configured."));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPromptForCredentialWithEmptyPasswd()
		{
			string[] args1 = new string[] { "create", "credential1", "-provider", jceksProvider
				 };
			AList<string> passwords = new AList<string>();
			passwords.AddItem(null);
			passwords.AddItem("p@ssw0rd");
			int rc = 0;
			CredentialShell shell = new CredentialShell();
			shell.SetConf(new Configuration());
			shell.SetPasswordReader(new TestCredShell.MockPasswordReader(this, passwords));
			rc = shell.Run(args1);
			Assert.Equal(outContent.ToString(), 1, rc);
			Assert.True(outContent.ToString().Contains("Passwords don't match"
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPromptForCredential()
		{
			string[] args1 = new string[] { "create", "credential1", "-provider", jceksProvider
				 };
			AList<string> passwords = new AList<string>();
			passwords.AddItem("p@ssw0rd");
			passwords.AddItem("p@ssw0rd");
			int rc = 0;
			CredentialShell shell = new CredentialShell();
			shell.SetConf(new Configuration());
			shell.SetPasswordReader(new TestCredShell.MockPasswordReader(this, passwords));
			rc = shell.Run(args1);
			Assert.Equal(0, rc);
			Assert.True(outContent.ToString().Contains("credential1 has been successfully "
				 + "created."));
			string[] args2 = new string[] { "delete", "credential1", "-f", "-provider", jceksProvider
				 };
			rc = shell.Run(args2);
			Assert.Equal(0, rc);
			Assert.True(outContent.ToString().Contains("credential1 has been successfully "
				 + "deleted."));
		}

		public class MockPasswordReader : CredentialShell.PasswordReader
		{
			internal IList<string> passwords = null;

			public MockPasswordReader(TestCredShell _enclosing, IList<string> passwds)
			{
				this._enclosing = _enclosing;
				this.passwords = passwds;
			}

			public override char[] ReadPassword(string prompt)
			{
				if (this.passwords.Count == 0)
				{
					return null;
				}
				string pass = this.passwords.Remove(0);
				return pass == null ? null : pass.ToCharArray();
			}

			public override void Format(string message)
			{
				System.Console.Out.WriteLine(message);
			}

			private readonly TestCredShell _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestEmptyArgList()
		{
			CredentialShell shell = new CredentialShell();
			shell.SetConf(new Configuration());
			Assert.Equal(1, shell.Init(new string[0]));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCommandHelpExitsNormally()
		{
			foreach (string cmd in Arrays.AsList("create", "list", "delete"))
			{
				CredentialShell shell = new CredentialShell();
				shell.SetConf(new Configuration());
				Assert.Equal("Expected help argument on " + cmd + " to return 0"
					, 0, shell.Init(new string[] { cmd, "-help" }));
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestEmptyArgForCommands()
		{
			CredentialShell shell = new CredentialShell();
			string[] command = new string[] { "list", "-provider" };
			Assert.Equal("Expected empty argument on " + command + " to return 1"
				, 1, shell.Init(command));
			foreach (string cmd in Arrays.AsList("create", "delete"))
			{
				shell.SetConf(new Configuration());
				Assert.Equal("Expected empty argument on " + cmd + " to return 1"
					, 1, shell.Init(new string[] { cmd }));
			}
		}
	}
}
