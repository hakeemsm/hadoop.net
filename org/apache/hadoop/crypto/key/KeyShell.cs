using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	/// <summary>This program is the CLI utility for the KeyProvider facilities in Hadoop.
	/// 	</summary>
	public class KeyShell : org.apache.hadoop.conf.Configured, org.apache.hadoop.util.Tool
	{
		private const string USAGE_PREFIX = "Usage: hadoop key " + "[generic options]\n";

		private const string COMMANDS = "   [-help]\n" + "   [" + org.apache.hadoop.crypto.key.KeyShell.CreateCommand
			.USAGE + "]\n" + "   [" + org.apache.hadoop.crypto.key.KeyShell.RollCommand.USAGE
			 + "]\n" + "   [" + org.apache.hadoop.crypto.key.KeyShell.DeleteCommand.USAGE + 
			"]\n" + "   [" + org.apache.hadoop.crypto.key.KeyShell.ListCommand.USAGE + "]\n";

		private const string LIST_METADATA = "keyShell.list.metadata";

		private bool interactive = true;

		private org.apache.hadoop.crypto.key.KeyShell.Command command = null;

		/// <summary>allows stdout to be captured if necessary</summary>
		public System.IO.TextWriter @out = System.Console.Out;

		/// <summary>allows stderr to be captured if necessary</summary>
		public System.IO.TextWriter err = System.Console.Error;

		private bool userSuppliedProvider = false;

		/// <summary>Primary entry point for the KeyShell; called via main().</summary>
		/// <param name="args">Command line arguments.</param>
		/// <returns>
		/// 0 on success and 1 on failure.  This value is passed back to
		/// the unix shell, so we must follow shell return code conventions:
		/// the return code is an unsigned character, and 0 means success, and
		/// small positive integers mean failure.
		/// </returns>
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
		/// % hadoop key create keyName [-size size] [-cipher algorithm]
		/// [-provider providerPath]
		/// % hadoop key roll keyName [-provider providerPath]
		/// % hadoop key list [-provider providerPath]
		/// % hadoop key delete keyName [-provider providerPath] [-i]
		/// </pre>
		/// </summary>
		/// <param name="args">Command line arguments.</param>
		/// <returns>0 on success, 1 on failure.</returns>
		/// <exception cref="System.IO.IOException"/>
		private int init(string[] args)
		{
			org.apache.hadoop.crypto.key.KeyProvider.Options options = org.apache.hadoop.crypto.key.KeyProvider
				.options(getConf());
			System.Collections.Generic.IDictionary<string, string> attributes = new System.Collections.Generic.Dictionary
				<string, string>();
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				bool moreTokens = (i < args.Length - 1);
				if (args[i].Equals("create"))
				{
					string keyName = "-help";
					if (moreTokens)
					{
						keyName = args[++i];
					}
					command = new org.apache.hadoop.crypto.key.KeyShell.CreateCommand(this, keyName, 
						options);
					if ("-help".Equals(keyName))
					{
						printKeyShellUsage();
						return 1;
					}
				}
				else
				{
					if (args[i].Equals("delete"))
					{
						string keyName = "-help";
						if (moreTokens)
						{
							keyName = args[++i];
						}
						command = new org.apache.hadoop.crypto.key.KeyShell.DeleteCommand(this, keyName);
						if ("-help".Equals(keyName))
						{
							printKeyShellUsage();
							return 1;
						}
					}
					else
					{
						if (args[i].Equals("roll"))
						{
							string keyName = "-help";
							if (moreTokens)
							{
								keyName = args[++i];
							}
							command = new org.apache.hadoop.crypto.key.KeyShell.RollCommand(this, keyName);
							if ("-help".Equals(keyName))
							{
								printKeyShellUsage();
								return 1;
							}
						}
						else
						{
							if ("list".Equals(args[i]))
							{
								command = new org.apache.hadoop.crypto.key.KeyShell.ListCommand(this);
							}
							else
							{
								if ("-size".Equals(args[i]) && moreTokens)
								{
									options.setBitLength(System.Convert.ToInt32(args[++i]));
								}
								else
								{
									if ("-cipher".Equals(args[i]) && moreTokens)
									{
										options.setCipher(args[++i]);
									}
									else
									{
										if ("-description".Equals(args[i]) && moreTokens)
										{
											options.setDescription(args[++i]);
										}
										else
										{
											if ("-attr".Equals(args[i]) && moreTokens)
											{
												string[] attrval = args[++i].split("=", 2);
												string attr = attrval[0].Trim();
												string val = attrval[1].Trim();
												if (attr.isEmpty() || val.isEmpty())
												{
													@out.WriteLine("\nAttributes must be in attribute=value form, " + "or quoted\nlike \"attribute = value\"\n"
														);
													printKeyShellUsage();
													return 1;
												}
												if (attributes.Contains(attr))
												{
													@out.WriteLine("\nEach attribute must correspond to only one value:\n" + "atttribute \""
														 + attr + "\" was repeated\n");
													printKeyShellUsage();
													return 1;
												}
												attributes[attr] = val;
											}
											else
											{
												if ("-provider".Equals(args[i]) && moreTokens)
												{
													userSuppliedProvider = true;
													getConf().set(org.apache.hadoop.crypto.key.KeyProviderFactory.KEY_PROVIDER_PATH, 
														args[++i]);
												}
												else
												{
													if ("-metadata".Equals(args[i]))
													{
														getConf().setBoolean(LIST_METADATA, true);
													}
													else
													{
														if ("-f".Equals(args[i]) || ("-force".Equals(args[i])))
														{
															interactive = false;
														}
														else
														{
															if ("-help".Equals(args[i]))
															{
																printKeyShellUsage();
																return 1;
															}
															else
															{
																printKeyShellUsage();
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
							}
						}
					}
				}
			}
			if (command == null)
			{
				printKeyShellUsage();
				return 1;
			}
			if (!attributes.isEmpty())
			{
				options.setAttributes(attributes);
			}
			return 0;
		}

		private void printKeyShellUsage()
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
				@out.WriteLine(org.apache.hadoop.crypto.key.KeyShell.CreateCommand.USAGE + ":\n\n"
					 + org.apache.hadoop.crypto.key.KeyShell.CreateCommand.DESC);
				@out.WriteLine("=========================================================" + "======"
					);
				@out.WriteLine(org.apache.hadoop.crypto.key.KeyShell.RollCommand.USAGE + ":\n\n" 
					+ org.apache.hadoop.crypto.key.KeyShell.RollCommand.DESC);
				@out.WriteLine("=========================================================" + "======"
					);
				@out.WriteLine(org.apache.hadoop.crypto.key.KeyShell.DeleteCommand.USAGE + ":\n\n"
					 + org.apache.hadoop.crypto.key.KeyShell.DeleteCommand.DESC);
				@out.WriteLine("=========================================================" + "======"
					);
				@out.WriteLine(org.apache.hadoop.crypto.key.KeyShell.ListCommand.USAGE + ":\n\n" 
					+ org.apache.hadoop.crypto.key.KeyShell.ListCommand.DESC);
			}
		}

		private abstract class Command
		{
			protected internal org.apache.hadoop.crypto.key.KeyProvider provider = null;

			public virtual bool validate()
			{
				return true;
			}

			protected internal virtual org.apache.hadoop.crypto.key.KeyProvider getKeyProvider
				()
			{
				org.apache.hadoop.crypto.key.KeyProvider provider = null;
				System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider> providers;
				try
				{
					providers = org.apache.hadoop.crypto.key.KeyProviderFactory.getProviders(this._enclosing
						.getConf());
					if (this._enclosing.userSuppliedProvider)
					{
						provider = providers[0];
					}
					else
					{
						foreach (org.apache.hadoop.crypto.key.KeyProvider p in providers)
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
				this._enclosing.@out.WriteLine(this.provider + " has been updated.");
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

			internal Command(KeyShell _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly KeyShell _enclosing;
		}

		private class ListCommand : org.apache.hadoop.crypto.key.KeyShell.Command
		{
			public const string USAGE = "list [-provider <provider>] [-metadata] [-help]";

			public const string DESC = "The list subcommand displays the keynames contained within\n"
				 + "a particular provider as configured in core-site.xml or\n" + "specified with the -provider argument. -metadata displays\n"
				 + "the metadata.";

			private bool metadata = false;

			public override bool validate()
			{
				bool rc = true;
				this.provider = this.getKeyProvider();
				if (this.provider == null)
				{
					this._enclosing.@out.WriteLine("There are no non-transient KeyProviders configured.\n"
						 + "Use the -provider option to specify a provider. If you\n" + "want to list a transient provider then you must use the\n"
						 + "-provider argument.");
					rc = false;
				}
				this.metadata = this._enclosing.getConf().getBoolean(org.apache.hadoop.crypto.key.KeyShell
					.LIST_METADATA, false);
				return rc;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void execute()
			{
				try
				{
					System.Collections.Generic.IList<string> keys = this.provider.getKeys();
					this._enclosing.@out.WriteLine("Listing keys for KeyProvider: " + this.provider);
					if (this.metadata)
					{
						org.apache.hadoop.crypto.key.KeyProvider.Metadata[] meta = this.provider.getKeysMetadata
							(Sharpen.Collections.ToArray(keys, new string[keys.Count]));
						for (int i = 0; i < meta.Length; ++i)
						{
							this._enclosing.@out.WriteLine(keys[i] + " : " + meta[i]);
						}
					}
					else
					{
						foreach (string keyName in keys)
						{
							this._enclosing.@out.WriteLine(keyName);
						}
					}
				}
				catch (System.IO.IOException e)
				{
					this._enclosing.@out.WriteLine("Cannot list keys for KeyProvider: " + this.provider
						 + ": " + e.ToString());
					throw;
				}
			}

			public override string getUsage()
			{
				return org.apache.hadoop.crypto.key.KeyShell.ListCommand.USAGE + ":\n\n" + org.apache.hadoop.crypto.key.KeyShell.ListCommand
					.DESC;
			}

			internal ListCommand(KeyShell _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly KeyShell _enclosing;
		}

		private class RollCommand : org.apache.hadoop.crypto.key.KeyShell.Command
		{
			public const string USAGE = "roll <keyname> [-provider <provider>] [-help]";

			public const string DESC = "The roll subcommand creates a new version for the specified key\n"
				 + "within the provider indicated using the -provider argument\n";

			internal string keyName = null;

			public RollCommand(KeyShell _enclosing, string keyName)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.keyName = keyName;
			}

			public override bool validate()
			{
				bool rc = true;
				this.provider = this.getKeyProvider();
				if (this.provider == null)
				{
					this._enclosing.@out.WriteLine("There are no valid KeyProviders configured. The key\n"
						 + "has not been rolled. Use the -provider option to specify\n" + "a provider.");
					rc = false;
				}
				if (this.keyName == null)
				{
					this._enclosing.@out.WriteLine("Please provide a <keyname>.\n" + "See the usage description by using -help."
						);
					rc = false;
				}
				return rc;
			}

			/// <exception cref="java.security.NoSuchAlgorithmException"/>
			/// <exception cref="System.IO.IOException"/>
			public override void execute()
			{
				try
				{
					this.warnIfTransientProvider();
					this._enclosing.@out.WriteLine("Rolling key version from KeyProvider: " + this.provider
						 + "\n  for key name: " + this.keyName);
					try
					{
						this.provider.rollNewVersion(this.keyName);
						this.provider.flush();
						this._enclosing.@out.WriteLine(this.keyName + " has been successfully rolled.");
						this.printProviderWritten();
					}
					catch (java.security.NoSuchAlgorithmException e)
					{
						this._enclosing.@out.WriteLine("Cannot roll key: " + this.keyName + " within KeyProvider: "
							 + this.provider + ". " + e.ToString());
						throw;
					}
				}
				catch (System.IO.IOException e1)
				{
					this._enclosing.@out.WriteLine("Cannot roll key: " + this.keyName + " within KeyProvider: "
						 + this.provider + ". " + e1.ToString());
					throw;
				}
			}

			public override string getUsage()
			{
				return org.apache.hadoop.crypto.key.KeyShell.RollCommand.USAGE + ":\n\n" + org.apache.hadoop.crypto.key.KeyShell.RollCommand
					.DESC;
			}

			private readonly KeyShell _enclosing;
		}

		private class DeleteCommand : org.apache.hadoop.crypto.key.KeyShell.Command
		{
			public const string USAGE = "delete <keyname> [-provider <provider>] [-f] [-help]";

			public const string DESC = "The delete subcommand deletes all versions of the key\n"
				 + "specified by the <keyname> argument from within the\n" + "provider specified -provider. The command asks for\n"
				 + "user confirmation unless -f is specified.";

			internal string keyName = null;

			internal bool cont = true;

			public DeleteCommand(KeyShell _enclosing, string keyName)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.keyName = keyName;
			}

			public override bool validate()
			{
				this.provider = this.getKeyProvider();
				if (this.provider == null)
				{
					this._enclosing.@out.WriteLine("There are no valid KeyProviders configured. Nothing\n"
						 + "was deleted. Use the -provider option to specify a provider.");
					return false;
				}
				if (this.keyName == null)
				{
					this._enclosing.@out.WriteLine("There is no keyName specified. Please specify a "
						 + "<keyname>. See the usage description with -help.");
					return false;
				}
				if (this._enclosing.interactive)
				{
					try
					{
						this.cont = org.apache.hadoop.util.ToolRunner.confirmPrompt("You are about to DELETE all versions of "
							 + " key " + this.keyName + " from KeyProvider " + this.provider + ". Continue? "
							);
						if (!this.cont)
						{
							this._enclosing.@out.WriteLine(this.keyName + " has not been deleted.");
						}
						return this.cont;
					}
					catch (System.IO.IOException e)
					{
						this._enclosing.@out.WriteLine(this.keyName + " will not be deleted.");
						Sharpen.Runtime.printStackTrace(e, this._enclosing.err);
					}
				}
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void execute()
			{
				this.warnIfTransientProvider();
				this._enclosing.@out.WriteLine("Deleting key: " + this.keyName + " from KeyProvider: "
					 + this.provider);
				if (this.cont)
				{
					try
					{
						this.provider.deleteKey(this.keyName);
						this.provider.flush();
						this._enclosing.@out.WriteLine(this.keyName + " has been successfully deleted.");
						this.printProviderWritten();
					}
					catch (System.IO.IOException e)
					{
						this._enclosing.@out.WriteLine(this.keyName + " has not been deleted. " + e.ToString
							());
						throw;
					}
				}
			}

			public override string getUsage()
			{
				return org.apache.hadoop.crypto.key.KeyShell.DeleteCommand.USAGE + ":\n\n" + org.apache.hadoop.crypto.key.KeyShell.DeleteCommand
					.DESC;
			}

			private readonly KeyShell _enclosing;
		}

		private class CreateCommand : org.apache.hadoop.crypto.key.KeyShell.Command
		{
			public const string USAGE = "create <keyname> [-cipher <cipher>] [-size <size>]\n"
				 + "                     [-description <description>]\n" + "                     [-attr <attribute=value>]\n"
				 + "                     [-provider <provider>] [-help]";

			public const string DESC = "The create subcommand creates a new key for the name specified\n"
				 + "by the <keyname> argument within the provider specified by the\n" + "-provider argument. You may specify a cipher with the -cipher\n"
				 + "argument. The default cipher is currently \"AES/CTR/NoPadding\".\n" + "The default keysize is 128. You may specify the requested key\n"
				 + "length using the -size argument. Arbitrary attribute=value\n" + "style attributes may be specified using the -attr argument.\n"
				 + "-attr may be specified multiple times, once per attribute.\n";

			internal readonly string keyName;

			internal readonly org.apache.hadoop.crypto.key.KeyProvider.Options options;

			public CreateCommand(KeyShell _enclosing, string keyName, org.apache.hadoop.crypto.key.KeyProvider.Options
				 options)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.keyName = keyName;
				this.options = options;
			}

			public override bool validate()
			{
				bool rc = true;
				this.provider = this.getKeyProvider();
				if (this.provider == null)
				{
					this._enclosing.@out.WriteLine("There are no valid KeyProviders configured. No key\n"
						 + " was created. You can use the -provider option to specify\n" + " a provider to use."
						);
					rc = false;
				}
				if (this.keyName == null)
				{
					this._enclosing.@out.WriteLine("Please provide a <keyname>. See the usage description"
						 + " with -help.");
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
					this.provider.createKey(this.keyName, this.options);
					this.provider.flush();
					this._enclosing.@out.WriteLine(this.keyName + " has been successfully created with options "
						 + this.options.ToString() + ".");
					this.printProviderWritten();
				}
				catch (java.security.InvalidParameterException e)
				{
					this._enclosing.@out.WriteLine(this.keyName + " has not been created. " + e.ToString
						());
					throw;
				}
				catch (System.IO.IOException e)
				{
					this._enclosing.@out.WriteLine(this.keyName + " has not been created. " + e.ToString
						());
					throw;
				}
				catch (java.security.NoSuchAlgorithmException e)
				{
					this._enclosing.@out.WriteLine(this.keyName + " has not been created. " + e.ToString
						());
					throw;
				}
			}

			public override string getUsage()
			{
				return org.apache.hadoop.crypto.key.KeyShell.CreateCommand.USAGE + ":\n\n" + org.apache.hadoop.crypto.key.KeyShell.CreateCommand
					.DESC;
			}

			private readonly KeyShell _enclosing;
		}

		/// <summary>main() entry point for the KeyShell.</summary>
		/// <remarks>
		/// main() entry point for the KeyShell.  While strictly speaking the
		/// return is void, it will System.exit() with a return code: 0 is for
		/// success and 1 for failure.
		/// </remarks>
		/// <param name="args">Command line arguments.</param>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = org.apache.hadoop.util.ToolRunner.run(new org.apache.hadoop.conf.Configuration
				(), new org.apache.hadoop.crypto.key.KeyShell(), args);
			System.Environment.Exit(res);
		}
	}
}
