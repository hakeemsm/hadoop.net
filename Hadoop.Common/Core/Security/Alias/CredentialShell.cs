using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Alias
{
	/// <summary>
	/// This program is the CLI utility for the CredentialProvider facilities in
	/// Hadoop.
	/// </summary>
	public class CredentialShell : Configured, Tool
	{
		private const string UsagePrefix = "Usage: hadoop credential " + "[generic options]\n";

		private const string Commands = "   [--help]\n" + "   [" + CredentialShell.CreateCommand
			.Usage + "]\n" + "   [" + CredentialShell.DeleteCommand.Usage + "]\n" + "   [" +
			 CredentialShell.ListCommand.Usage + "]\n";

		private bool interactive = true;

		private CredentialShell.Command command = null;

		/// <summary>allows stdout to be captured if necessary</summary>
		public TextWriter @out = System.Console.Out;

		/// <summary>allows stderr to be captured if necessary</summary>
		public TextWriter err = System.Console.Error;

		private bool userSuppliedProvider = false;

		private string value = null;

		private CredentialShell.PasswordReader passwordReader;

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			int exitCode = 0;
			try
			{
				exitCode = Init(args);
				if (exitCode != 0)
				{
					return exitCode;
				}
				if (command.Validate())
				{
					command.Execute();
				}
				else
				{
					exitCode = 1;
				}
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e, err);
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
		protected internal virtual int Init(string[] args)
		{
			// no args should print the help message
			if (0 == args.Length)
			{
				PrintCredShellUsage();
				ToolRunner.PrintGenericCommandUsage(System.Console.Error);
				return 1;
			}
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (args[i].Equals("create"))
				{
					if (i == args.Length - 1)
					{
						PrintCredShellUsage();
						return 1;
					}
					string alias = args[++i];
					command = new CredentialShell.CreateCommand(this, alias);
					if (alias.Equals("-help"))
					{
						PrintCredShellUsage();
						return 0;
					}
				}
				else
				{
					if (args[i].Equals("delete"))
					{
						if (i == args.Length - 1)
						{
							PrintCredShellUsage();
							return 1;
						}
						string alias = args[++i];
						command = new CredentialShell.DeleteCommand(this, alias);
						if (alias.Equals("-help"))
						{
							PrintCredShellUsage();
							return 0;
						}
					}
					else
					{
						if (args[i].Equals("list"))
						{
							command = new CredentialShell.ListCommand(this);
						}
						else
						{
							if (args[i].Equals("-provider"))
							{
								if (i == args.Length - 1)
								{
									PrintCredShellUsage();
									return 1;
								}
								userSuppliedProvider = true;
								GetConf().Set(CredentialProviderFactory.CredentialProviderPath, args[++i]);
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
											PrintCredShellUsage();
											return 0;
										}
										else
										{
											PrintCredShellUsage();
											ToolRunner.PrintGenericCommandUsage(System.Console.Error);
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

		private void PrintCredShellUsage()
		{
			@out.WriteLine(UsagePrefix + Commands);
			if (command != null)
			{
				@out.WriteLine(command.GetUsage());
			}
			else
			{
				@out.WriteLine("=========================================================" + "======"
					);
				@out.WriteLine(CredentialShell.CreateCommand.Usage + ":\n\n" + CredentialShell.CreateCommand
					.Desc);
				@out.WriteLine("=========================================================" + "======"
					);
				@out.WriteLine(CredentialShell.DeleteCommand.Usage + ":\n\n" + CredentialShell.DeleteCommand
					.Desc);
				@out.WriteLine("=========================================================" + "======"
					);
				@out.WriteLine(CredentialShell.ListCommand.Usage + ":\n\n" + CredentialShell.ListCommand
					.Desc);
			}
		}

		private abstract class Command
		{
			protected internal CredentialProvider provider = null;

			public virtual bool Validate()
			{
				return true;
			}

			protected internal virtual CredentialProvider GetCredentialProvider()
			{
				CredentialProvider provider = null;
				IList<CredentialProvider> providers;
				try
				{
					providers = CredentialProviderFactory.GetProviders(this._enclosing.GetConf());
					if (this._enclosing.userSuppliedProvider)
					{
						provider = providers[0];
					}
					else
					{
						foreach (CredentialProvider p in providers)
						{
							if (!p.IsTransient())
							{
								provider = p;
								break;
							}
						}
					}
				}
				catch (IOException e)
				{
					Sharpen.Runtime.PrintStackTrace(e, this._enclosing.err);
				}
				return provider;
			}

			protected internal virtual void PrintProviderWritten()
			{
				this._enclosing.@out.WriteLine(this.provider.GetType().FullName + " has been updated."
					);
			}

			protected internal virtual void WarnIfTransientProvider()
			{
				if (this.provider.IsTransient())
				{
					this._enclosing.@out.WriteLine("WARNING: you are modifying a transient provider."
						);
				}
			}

			/// <exception cref="System.Exception"/>
			public abstract void Execute();

			public abstract string GetUsage();

			internal Command(CredentialShell _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly CredentialShell _enclosing;
		}

		private class ListCommand : CredentialShell.Command
		{
			public const string Usage = "list [-provider provider-path]";

			public const string Desc = "The list subcommand displays the aliases contained within \n"
				 + "a particular provider - as configured in core-site.xml or " + "indicated\nthrough the -provider argument.";

			public override bool Validate()
			{
				bool rc = true;
				this.provider = this.GetCredentialProvider();
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
			public override void Execute()
			{
				IList<string> aliases;
				try
				{
					aliases = this.provider.GetAliases();
					this._enclosing.@out.WriteLine("Listing aliases for CredentialProvider: " + this.
						provider.ToString());
					foreach (string alias in aliases)
					{
						this._enclosing.@out.WriteLine(alias);
					}
				}
				catch (IOException e)
				{
					this._enclosing.@out.WriteLine("Cannot list aliases for CredentialProvider: " + this
						.provider.ToString() + ": " + e.Message);
					throw;
				}
			}

			public override string GetUsage()
			{
				return CredentialShell.ListCommand.Usage + ":\n\n" + CredentialShell.ListCommand.
					Desc;
			}

			internal ListCommand(CredentialShell _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly CredentialShell _enclosing;
		}

		private class DeleteCommand : CredentialShell.Command
		{
			public const string Usage = "delete <alias> [-f] [-provider provider-path]";

			public const string Desc = "The delete subcommand deletes the credential\n" + "specified as the <alias> argument from within the provider\n"
				 + "indicated through the -provider argument. The command asks for\n" + "confirmation unless the -f option is specified.";

			internal string alias = null;

			internal bool cont = true;

			public DeleteCommand(CredentialShell _enclosing, string alias)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.alias = alias;
			}

			public override bool Validate()
			{
				this.provider = this.GetCredentialProvider();
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
						this.cont = ToolRunner.ConfirmPrompt("You are about to DELETE the credential " + 
							this.alias + " from CredentialProvider " + this.provider.ToString() + ". Continue? "
							);
						if (!this.cont)
						{
							this._enclosing.@out.WriteLine("Nothing has been be deleted.");
						}
						return this.cont;
					}
					catch (IOException e)
					{
						this._enclosing.@out.WriteLine(this.alias + " will not be deleted.");
						Sharpen.Runtime.PrintStackTrace(e, this._enclosing.err);
					}
				}
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Execute()
			{
				this.WarnIfTransientProvider();
				this._enclosing.@out.WriteLine("Deleting credential: " + this.alias + " from CredentialProvider: "
					 + this.provider.ToString());
				if (this.cont)
				{
					try
					{
						this.provider.DeleteCredentialEntry(this.alias);
						this._enclosing.@out.WriteLine(this.alias + " has been successfully deleted.");
						this.provider.Flush();
						this.PrintProviderWritten();
					}
					catch (IOException e)
					{
						this._enclosing.@out.WriteLine(this.alias + " has NOT been deleted.");
						throw;
					}
				}
			}

			public override string GetUsage()
			{
				return CredentialShell.DeleteCommand.Usage + ":\n\n" + CredentialShell.DeleteCommand
					.Desc;
			}

			private readonly CredentialShell _enclosing;
		}

		private class CreateCommand : CredentialShell.Command
		{
			public const string Usage = "create <alias> [-provider provider-path]";

			public const string Desc = "The create subcommand creates a new credential for the name specified\n"
				 + "as the <alias> argument within the provider indicated through\n" + "the -provider argument.";

			internal string alias = null;

			public CreateCommand(CredentialShell _enclosing, string alias)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.alias = alias;
			}

			public override bool Validate()
			{
				bool rc = true;
				this.provider = this.GetCredentialProvider();
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
			/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
			public override void Execute()
			{
				this.WarnIfTransientProvider();
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
						credential = this._enclosing.PromptForCredential();
					}
					this.provider.CreateCredentialEntry(this.alias, credential);
					this._enclosing.@out.WriteLine(this.alias + " has been successfully created.");
					this.provider.Flush();
					this.PrintProviderWritten();
				}
				catch (InvalidParameterException e)
				{
					this._enclosing.@out.WriteLine(this.alias + " has NOT been created. " + e.Message
						);
					throw;
				}
				catch (IOException e)
				{
					this._enclosing.@out.WriteLine(this.alias + " has NOT been created. " + e.Message
						);
					throw;
				}
			}

			public override string GetUsage()
			{
				return CredentialShell.CreateCommand.Usage + ":\n\n" + CredentialShell.CreateCommand
					.Desc;
			}

			private readonly CredentialShell _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual char[] PromptForCredential()
		{
			CredentialShell.PasswordReader c = GetPasswordReader();
			if (c == null)
			{
				throw new IOException("No console available for prompting user.");
			}
			char[] cred = null;
			bool noMatch;
			do
			{
				char[] newPassword1 = c.ReadPassword("Enter password: ");
				char[] newPassword2 = c.ReadPassword("Enter password again: ");
				noMatch = !Arrays.Equals(newPassword1, newPassword2);
				if (noMatch)
				{
					if (newPassword1 != null)
					{
						Arrays.Fill(newPassword1, ' ');
					}
					c.Format("Passwords don't match. Try again.%n");
				}
				else
				{
					cred = newPassword1;
				}
				if (newPassword2 != null)
				{
					Arrays.Fill(newPassword2, ' ');
				}
			}
			while (noMatch);
			return cred;
		}

		public virtual CredentialShell.PasswordReader GetPasswordReader()
		{
			if (passwordReader == null)
			{
				passwordReader = new CredentialShell.PasswordReader();
			}
			return passwordReader;
		}

		public virtual void SetPasswordReader(CredentialShell.PasswordReader reader)
		{
			passwordReader = reader;
		}

		public class PasswordReader
		{
			// to facilitate testing since Console is a final class...
			public virtual char[] ReadPassword(string prompt)
			{
				Console console = Runtime.Console();
				char[] pass = console.ReadPassword(prompt);
				return pass;
			}

			public virtual void Format(string message)
			{
				Console console = Runtime.Console();
				console.Format(message);
			}
		}

		/// <summary>Main program.</summary>
		/// <param name="args">Command line arguments</param>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new CredentialShell(), args);
			System.Environment.Exit(res);
		}
	}
}
