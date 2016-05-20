using Sharpen;

namespace org.apache.hadoop.security.alias
{
	/// <summary>
	/// This program is the CLI utility for the CredentialProvider facilities in
	/// Hadoop.
	/// </summary>
	public class CredentialShell : org.apache.hadoop.conf.Configured, org.apache.hadoop.util.Tool
	{
		private const string USAGE_PREFIX = "Usage: hadoop credential " + "[generic options]\n";

		private const string COMMANDS = "   [--help]\n" + "   [" + org.apache.hadoop.security.alias.CredentialShell.CreateCommand
			.USAGE + "]\n" + "   [" + org.apache.hadoop.security.alias.CredentialShell.DeleteCommand
			.USAGE + "]\n" + "   [" + org.apache.hadoop.security.alias.CredentialShell.ListCommand
			.USAGE + "]\n";

		private bool interactive = true;

		private org.apache.hadoop.security.alias.CredentialShell.Command command = null;

		/// <summary>allows stdout to be captured if necessary</summary>
		public System.IO.TextWriter @out = System.Console.Out;

		/// <summary>allows stderr to be captured if necessary</summary>
		public System.IO.TextWriter err = System.Console.Error;

		private bool userSuppliedProvider = false;

		private string value = null;

		private org.apache.hadoop.security.alias.CredentialShell.PasswordReader passwordReader;

		/// <exception cref="System.Exception"/>
		public virtual int run(string[] args)
		{
			int exitCode = 0;
			try
			{
				exitCode = init(args);
				if (exitCode != 0)
				{
					return exitCode;
				}
				if (command.validate())
				{
					command.execute();
				}
				else
				{
					exitCode = 1;
				}
			}
			catch (System.Exception e)
			{
				Sharpen.Runtime.printStackTrace(e, err);
				return 1;
			}
			return exitCode;
		}

		/// <summary>
		/// Parse the command line arguments and initialize the data
		/// <pre>
		/// % hadoop credential create alias [-provider providerPath]
		/// % hadoop credential list [-provider providerPath]
		/// % hadoop credential delete alias [-provider providerPath] [-f]
		/// </pre>
		/// </summary>
		/// <param name="args"/>
		/// <returns>0 if the argument(s) were recognized, 1 otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual int init(string[] args)
		{
			// no args should print the help message
			if (0 == args.Length)
			{
				printCredShellUsage();
				org.apache.hadoop.util.ToolRunner.printGenericCommandUsage(System.Console.Error);
				return 1;
			}
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (args[i].Equals("create"))
				{
					if (i == args.Length - 1)
					{
						printCredShellUsage();
						return 1;
					}
					string alias = args[++i];
					command = new org.apache.hadoop.security.alias.CredentialShell.CreateCommand(this
						, alias);
					if (alias.Equals("-help"))
					{
						printCredShellUsage();
						return 0;
					}
				}
				else
				{
					if (args[i].Equals("delete"))
					{
						if (i == args.Length - 1)
						{
							printCredShellUsage();
							return 1;
						}
						string alias = args[++i];
						command = new org.apache.hadoop.security.alias.CredentialShell.DeleteCommand(this
							, alias);
						if (alias.Equals("-help"))
						{
							printCredShellUsage();
							return 0;
						}
					}
					else
					{
						if (args[i].Equals("list"))
						{
							command = new org.apache.hadoop.security.alias.CredentialShell.ListCommand(this);
						}
						else
						{
							if (args[i].Equals("-provider"))
							{
								if (i == args.Length - 1)
								{
									printCredShellUsage();
									return 1;
								}
								userSuppliedProvider = true;
								getConf().set(org.apache.hadoop.security.alias.CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH
									, args[++i]);
							}
							else
							{
								if (args[i].Equals("-f") || (args[i].Equals("-force")))
								{
									interactive = false;
								}
								else
								{
									if (args[i].Equals("-v") || (args[i].Equals("-value")))
									{
										value = args[++i];
									}
									else
									{
										if (args[i].Equals("-help"))
										{
											printCredShellUsage();
											return 0;
										}
										else
										{
											printCredShellUsage();
											org.apache.hadoop.util.ToolRunner.printGenericCommandUsage(System.Console.Error);
											return 1;
										}
									}
								}
							}
						}
					}
				}
			}
			return 0;
		}

		private void printCredShellUsage()
		{
			@out.WriteLine(USAGE_PREFIX + COMMANDS);
			if (command != null)
			{
				@out.WriteLine(command.getUsage());
			}
			else
			{
				@out.WriteLine("=========================================================" + "======"
					);
				@out.WriteLine(org.apache.hadoop.security.alias.CredentialShell.CreateCommand.USAGE
					 + ":\n\n" + org.apache.hadoop.security.alias.CredentialShell.CreateCommand.DESC
					);
				@out.WriteLine("=========================================================" + "======"
					);
				@out.WriteLine(org.apache.hadoop.security.alias.CredentialShell.DeleteCommand.USAGE
					 + ":\n\n" + org.apache.hadoop.security.alias.CredentialShell.DeleteCommand.DESC
					);
				@out.WriteLine("=========================================================" + "======"
					);
				@out.WriteLine(org.apache.hadoop.security.alias.CredentialShell.ListCommand.USAGE
					 + ":\n\n" + org.apache.hadoop.security.alias.CredentialShell.ListCommand.DESC);
			}
		}

		private abstract class Command
		{
			protected internal org.apache.hadoop.security.alias.CredentialProvider provider = 
				null;

			public virtual bool validate()
			{
				return true;
			}

			protected internal virtual org.apache.hadoop.security.alias.CredentialProvider getCredentialProvider
				()
			{
				org.apache.hadoop.security.alias.CredentialProvider provider = null;
				System.Collections.Generic.IList<org.apache.hadoop.security.alias.CredentialProvider
					> providers;
				try
				{
					providers = org.apache.hadoop.security.alias.CredentialProviderFactory.getProviders
						(this._enclosing.getConf());
					if (this._enclosing.userSuppliedProvider)
					{
						provider = providers[0];
					}
					else
					{
						foreach (org.apache.hadoop.security.alias.CredentialProvider p in providers)
						{
							if (!p.isTransient())
							{
								provider = p;
								break;
							}
						}
					}
				}
				catch (System.IO.IOException e)
				{
					Sharpen.Runtime.printStackTrace(e, this._enclosing.err);
				}
				return provider;
			}

			protected internal virtual void printProviderWritten()
			{
				this._enclosing.@out.WriteLine(Sharpen.Runtime.getClassForObject(this.provider).getName
					() + " has been updated.");
			}

			protected internal virtual void warnIfTransientProvider()
			{
				if (this.provider.isTransient())
				{
					this._enclosing.@out.WriteLine("WARNING: you are modifying a transient provider."
						);
				}
			}

			/// <exception cref="System.Exception"/>
			public abstract void execute();

			public abstract string getUsage();

			internal Command(CredentialShell _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly CredentialShell _enclosing;
		}

		private class ListCommand : org.apache.hadoop.security.alias.CredentialShell.Command
		{
			public const string USAGE = "list [-provider provider-path]";

			public const string DESC = "The list subcommand displays the aliases contained within \n"
				 + "a particular provider - as configured in core-site.xml or " + "indicated\nthrough the -provider argument.";

			public override bool validate()
			{
				bool rc = true;
				this.provider = this.getCredentialProvider();
				if (this.provider == null)
				{
					this._enclosing.@out.WriteLine("There are no non-transient CredentialProviders configured.\n"
						 + "Consider using the -provider option to indicate the provider\n" + "to use. If you want to list a transient provider then you\n"
						 + "you MUST use the -provider argument.");
					rc = false;
				}
				return rc;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void execute()
			{
				System.Collections.Generic.IList<string> aliases;
				try
				{
					aliases = this.provider.getAliases();
					this._enclosing.@out.WriteLine("Listing aliases for CredentialProvider: " + this.
						provider.ToString());
					foreach (string alias in aliases)
					{
						this._enclosing.@out.WriteLine(alias);
					}
				}
				catch (System.IO.IOException e)
				{
					this._enclosing.@out.WriteLine("Cannot list aliases for CredentialProvider: " + this
						.provider.ToString() + ": " + e.Message);
					throw;
				}
			}

			public override string getUsage()
			{
				return org.apache.hadoop.security.alias.CredentialShell.ListCommand.USAGE + ":\n\n"
					 + org.apache.hadoop.security.alias.CredentialShell.ListCommand.DESC;
			}

			internal ListCommand(CredentialShell _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly CredentialShell _enclosing;
		}

		private class DeleteCommand : org.apache.hadoop.security.alias.CredentialShell.Command
		{
			public const string USAGE = "delete <alias> [-f] [-provider provider-path]";

			public const string DESC = "The delete subcommand deletes the credential\n" + "specified as the <alias> argument from within the provider\n"
				 + "indicated through the -provider argument. The command asks for\n" + "confirmation unless the -f option is specified.";

			internal string alias = null;

			internal bool cont = true;

			public DeleteCommand(CredentialShell _enclosing, string alias)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.alias = alias;
			}

			public override bool validate()
			{
				this.provider = this.getCredentialProvider();
				if (this.provider == null)
				{
					this._enclosing.@out.WriteLine("There are no valid CredentialProviders configured.\n"
						 + "Nothing will be deleted.\n" + "Consider using the -provider option to indicate the provider"
						 + " to use.");
					return false;
				}
				if (this.alias == null)
				{
					this._enclosing.@out.WriteLine("There is no alias specified. Please provide the" 
						+ "mandatory <alias>. See the usage description with -help.");
					return false;
				}
				if (this._enclosing.interactive)
				{
					try
					{
						this.cont = org.apache.hadoop.util.ToolRunner.confirmPrompt("You are about to DELETE the credential "
							 + this.alias + " from CredentialProvider " + this.provider.ToString() + ". Continue? "
							);
						if (!this.cont)
						{
							this._enclosing.@out.WriteLine("Nothing has been be deleted.");
						}
						return this.cont;
					}
					catch (System.IO.IOException e)
					{
						this._enclosing.@out.WriteLine(this.alias + " will not be deleted.");
						Sharpen.Runtime.printStackTrace(e, this._enclosing.err);
					}
				}
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void execute()
			{
				this.warnIfTransientProvider();
				this._enclosing.@out.WriteLine("Deleting credential: " + this.alias + " from CredentialProvider: "
					 + this.provider.ToString());
				if (this.cont)
				{
					try
					{
						this.provider.deleteCredentialEntry(this.alias);
						this._enclosing.@out.WriteLine(this.alias + " has been successfully deleted.");
						this.provider.flush();
						this.printProviderWritten();
					}
					catch (System.IO.IOException e)
					{
						this._enclosing.@out.WriteLine(this.alias + " has NOT been deleted.");
						throw;
					}
				}
			}

			public override string getUsage()
			{
				return org.apache.hadoop.security.alias.CredentialShell.DeleteCommand.USAGE + ":\n\n"
					 + org.apache.hadoop.security.alias.CredentialShell.DeleteCommand.DESC;
			}

			private readonly CredentialShell _enclosing;
		}

		private class CreateCommand : org.apache.hadoop.security.alias.CredentialShell.Command
		{
			public const string USAGE = "create <alias> [-provider provider-path]";

			public const string DESC = "The create subcommand creates a new credential for the name specified\n"
				 + "as the <alias> argument within the provider indicated through\n" + "the -provider argument.";

			internal string alias = null;

			public CreateCommand(CredentialShell _enclosing, string alias)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.alias = alias;
			}

			public override bool validate()
			{
				bool rc = true;
				this.provider = this.getCredentialProvider();
				if (this.provider == null)
				{
					this._enclosing.@out.WriteLine("There are no valid CredentialProviders configured."
						 + "\nCredential will not be created.\n" + "Consider using the -provider option to indicate the provider"
						 + " to use.");
					rc = false;
				}
				if (this.alias == null)
				{
					this._enclosing.@out.WriteLine("There is no alias specified. Please provide the" 
						+ "mandatory <alias>. See the usage description with -help.");
					rc = false;
				}
				return rc;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="java.security.NoSuchAlgorithmException"/>
			public override void execute()
			{
				this.warnIfTransientProvider();
				try
				{
					char[] credential = null;
					if (this._enclosing.value != null)
					{
						// testing only
						credential = this._enclosing.value.ToCharArray();
					}
					else
					{
						credential = this._enclosing.promptForCredential();
					}
					this.provider.createCredentialEntry(this.alias, credential);
					this._enclosing.@out.WriteLine(this.alias + " has been successfully created.");
					this.provider.flush();
					this.printProviderWritten();
				}
				catch (java.security.InvalidParameterException e)
				{
					this._enclosing.@out.WriteLine(this.alias + " has NOT been created. " + e.Message
						);
					throw;
				}
				catch (System.IO.IOException e)
				{
					this._enclosing.@out.WriteLine(this.alias + " has NOT been created. " + e.Message
						);
					throw;
				}
			}

			public override string getUsage()
			{
				return org.apache.hadoop.security.alias.CredentialShell.CreateCommand.USAGE + ":\n\n"
					 + org.apache.hadoop.security.alias.CredentialShell.CreateCommand.DESC;
			}

			private readonly CredentialShell _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual char[] promptForCredential()
		{
			org.apache.hadoop.security.alias.CredentialShell.PasswordReader c = getPasswordReader
				();
			if (c == null)
			{
				throw new System.IO.IOException("No console available for prompting user.");
			}
			char[] cred = null;
			bool noMatch;
			do
			{
				char[] newPassword1 = c.readPassword("Enter password: ");
				char[] newPassword2 = c.readPassword("Enter password again: ");
				noMatch = !java.util.Arrays.equals(newPassword1, newPassword2);
				if (noMatch)
				{
					if (newPassword1 != null)
					{
						java.util.Arrays.fill(newPassword1, ' ');
					}
					c.format("Passwords don't match. Try again.%n");
				}
				else
				{
					cred = newPassword1;
				}
				if (newPassword2 != null)
				{
					java.util.Arrays.fill(newPassword2, ' ');
				}
			}
			while (noMatch);
			return cred;
		}

		public virtual org.apache.hadoop.security.alias.CredentialShell.PasswordReader getPasswordReader
			()
		{
			if (passwordReader == null)
			{
				passwordReader = new org.apache.hadoop.security.alias.CredentialShell.PasswordReader
					();
			}
			return passwordReader;
		}

		public virtual void setPasswordReader(org.apache.hadoop.security.alias.CredentialShell.PasswordReader
			 reader)
		{
			passwordReader = reader;
		}

		public class PasswordReader
		{
			// to facilitate testing since Console is a final class...
			public virtual char[] readPassword(string prompt)
			{
				java.io.Console console = Sharpen.Runtime.console();
				char[] pass = console.readPassword(prompt);
				return pass;
			}

			public virtual void format(string message)
			{
				java.io.Console console = Sharpen.Runtime.console();
				console.format(message);
			}
		}

		/// <summary>Main program.</summary>
		/// <param name="args">Command line arguments</param>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = org.apache.hadoop.util.ToolRunner.run(new org.apache.hadoop.conf.Configuration
				(), new org.apache.hadoop.security.alias.CredentialShell(), args);
			System.Environment.Exit(res);
		}
	}
}
