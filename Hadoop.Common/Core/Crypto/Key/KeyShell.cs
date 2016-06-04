using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key
{
	/// <summary>This program is the CLI utility for the KeyProvider facilities in Hadoop.
	/// 	</summary>
	public class KeyShell : Configured, Tool
	{
		private const string UsagePrefix = "Usage: hadoop key " + "[generic options]\n";

		private const string Commands = "   [-help]\n" + "   [" + KeyShell.CreateCommand.
			Usage + "]\n" + "   [" + KeyShell.RollCommand.Usage + "]\n" + "   [" + KeyShell.DeleteCommand
			.Usage + "]\n" + "   [" + KeyShell.ListCommand.Usage + "]\n";

		private const string ListMetadata = "keyShell.list.metadata";

		private bool interactive = true;

		private KeyShell.Command command = null;

		/// <summary>allows stdout to be captured if necessary</summary>
		public TextWriter @out = System.Console.Out;

		/// <summary>allows stderr to be captured if necessary</summary>
		public TextWriter err = System.Console.Error;

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
		private int Init(string[] args)
		{
			KeyProvider.Options options = KeyProvider.Options(GetConf());
			IDictionary<string, string> attributes = new Dictionary<string, string>();
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
					command = new KeyShell.CreateCommand(this, keyName, options);
					if ("-help".Equals(keyName))
					{
						PrintKeyShellUsage();
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
						command = new KeyShell.DeleteCommand(this, keyName);
						if ("-help".Equals(keyName))
						{
							PrintKeyShellUsage();
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
							command = new KeyShell.RollCommand(this, keyName);
							if ("-help".Equals(keyName))
							{
								PrintKeyShellUsage();
								return 1;
							}
						}
						else
						{
							if ("list".Equals(args[i]))
							{
								command = new KeyShell.ListCommand(this);
							}
							else
							{
								if ("-size".Equals(args[i]) && moreTokens)
								{
									options.SetBitLength(System.Convert.ToInt32(args[++i]));
								}
								else
								{
									if ("-cipher".Equals(args[i]) && moreTokens)
									{
										options.SetCipher(args[++i]);
									}
									else
									{
										if ("-description".Equals(args[i]) && moreTokens)
										{
											options.SetDescription(args[++i]);
										}
										else
										{
											if ("-attr".Equals(args[i]) && moreTokens)
											{
												string[] attrval = args[++i].Split("=", 2);
												string attr = attrval[0].Trim();
												string val = attrval[1].Trim();
												if (attr.IsEmpty() || val.IsEmpty())
												{
													@out.WriteLine("\nAttributes must be in attribute=value form, " + "or quoted\nlike \"attribute = value\"\n"
														);
													PrintKeyShellUsage();
													return 1;
												}
												if (attributes.Contains(attr))
												{
													@out.WriteLine("\nEach attribute must correspond to only one value:\n" + "atttribute \""
														 + attr + "\" was repeated\n");
													PrintKeyShellUsage();
													return 1;
												}
												attributes[attr] = val;
											}
											else
											{
												if ("-provider".Equals(args[i]) && moreTokens)
												{
													userSuppliedProvider = true;
													GetConf().Set(KeyProviderFactory.KeyProviderPath, args[++i]);
												}
												else
												{
													if ("-metadata".Equals(args[i]))
													{
														GetConf().SetBoolean(ListMetadata, true);
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
																PrintKeyShellUsage();
																return 1;
															}
															else
															{
																PrintKeyShellUsage();
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
							}
						}
					}
				}
			}
			if (command == null)
			{
				PrintKeyShellUsage();
				return 1;
			}
			if (!attributes.IsEmpty())
			{
				options.SetAttributes(attributes);
			}
			return 0;
		}

		private void PrintKeyShellUsage()
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
				@out.WriteLine(KeyShell.CreateCommand.Usage + ":\n\n" + KeyShell.CreateCommand.Desc
					);
				@out.WriteLine("=========================================================" + "======"
					);
				@out.WriteLine(KeyShell.RollCommand.Usage + ":\n\n" + KeyShell.RollCommand.Desc);
				@out.WriteLine("=========================================================" + "======"
					);
				@out.WriteLine(KeyShell.DeleteCommand.Usage + ":\n\n" + KeyShell.DeleteCommand.Desc
					);
				@out.WriteLine("=========================================================" + "======"
					);
				@out.WriteLine(KeyShell.ListCommand.Usage + ":\n\n" + KeyShell.ListCommand.Desc);
			}
		}

		private abstract class Command
		{
			protected internal KeyProvider provider = null;

			public virtual bool Validate()
			{
				return true;
			}

			protected internal virtual KeyProvider GetKeyProvider()
			{
				KeyProvider provider = null;
				IList<KeyProvider> providers;
				try
				{
					providers = KeyProviderFactory.GetProviders(this._enclosing.GetConf());
					if (this._enclosing.userSuppliedProvider)
					{
						provider = providers[0];
					}
					else
					{
						foreach (KeyProvider p in providers)
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
				this._enclosing.@out.WriteLine(this.provider + " has been updated.");
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

			internal Command(KeyShell _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly KeyShell _enclosing;
		}

		private class ListCommand : KeyShell.Command
		{
			public const string Usage = "list [-provider <provider>] [-metadata] [-help]";

			public const string Desc = "The list subcommand displays the keynames contained within\n"
				 + "a particular provider as configured in core-site.xml or\n" + "specified with the -provider argument. -metadata displays\n"
				 + "the metadata.";

			private bool metadata = false;

			public override bool Validate()
			{
				bool rc = true;
				this.provider = this.GetKeyProvider();
				if (this.provider == null)
				{
					this._enclosing.@out.WriteLine("There are no non-transient KeyProviders configured.\n"
						 + "Use the -provider option to specify a provider. If you\n" + "want to list a transient provider then you must use the\n"
						 + "-provider argument.");
					rc = false;
				}
				this.metadata = this._enclosing.GetConf().GetBoolean(KeyShell.ListMetadata, false
					);
				return rc;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Execute()
			{
				try
				{
					IList<string> keys = this.provider.GetKeys();
					this._enclosing.@out.WriteLine("Listing keys for KeyProvider: " + this.provider);
					if (this.metadata)
					{
						KeyProvider.Metadata[] meta = this.provider.GetKeysMetadata(Sharpen.Collections.ToArray
							(keys, new string[keys.Count]));
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
				catch (IOException e)
				{
					this._enclosing.@out.WriteLine("Cannot list keys for KeyProvider: " + this.provider
						 + ": " + e.ToString());
					throw;
				}
			}

			public override string GetUsage()
			{
				return KeyShell.ListCommand.Usage + ":\n\n" + KeyShell.ListCommand.Desc;
			}

			internal ListCommand(KeyShell _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly KeyShell _enclosing;
		}

		private class RollCommand : KeyShell.Command
		{
			public const string Usage = "roll <keyname> [-provider <provider>] [-help]";

			public const string Desc = "The roll subcommand creates a new version for the specified key\n"
				 + "within the provider indicated using the -provider argument\n";

			internal string keyName = null;

			public RollCommand(KeyShell _enclosing, string keyName)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.keyName = keyName;
			}

			public override bool Validate()
			{
				bool rc = true;
				this.provider = this.GetKeyProvider();
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

			/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
			/// <exception cref="System.IO.IOException"/>
			public override void Execute()
			{
				try
				{
					this.WarnIfTransientProvider();
					this._enclosing.@out.WriteLine("Rolling key version from KeyProvider: " + this.provider
						 + "\n  for key name: " + this.keyName);
					try
					{
						this.provider.RollNewVersion(this.keyName);
						this.provider.Flush();
						this._enclosing.@out.WriteLine(this.keyName + " has been successfully rolled.");
						this.PrintProviderWritten();
					}
					catch (NoSuchAlgorithmException e)
					{
						this._enclosing.@out.WriteLine("Cannot roll key: " + this.keyName + " within KeyProvider: "
							 + this.provider + ". " + e.ToString());
						throw;
					}
				}
				catch (IOException e1)
				{
					this._enclosing.@out.WriteLine("Cannot roll key: " + this.keyName + " within KeyProvider: "
						 + this.provider + ". " + e1.ToString());
					throw;
				}
			}

			public override string GetUsage()
			{
				return KeyShell.RollCommand.Usage + ":\n\n" + KeyShell.RollCommand.Desc;
			}

			private readonly KeyShell _enclosing;
		}

		private class DeleteCommand : KeyShell.Command
		{
			public const string Usage = "delete <keyname> [-provider <provider>] [-f] [-help]";

			public const string Desc = "The delete subcommand deletes all versions of the key\n"
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

			public override bool Validate()
			{
				this.provider = this.GetKeyProvider();
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
						this.cont = ToolRunner.ConfirmPrompt("You are about to DELETE all versions of " +
							 " key " + this.keyName + " from KeyProvider " + this.provider + ". Continue? ");
						if (!this.cont)
						{
							this._enclosing.@out.WriteLine(this.keyName + " has not been deleted.");
						}
						return this.cont;
					}
					catch (IOException e)
					{
						this._enclosing.@out.WriteLine(this.keyName + " will not be deleted.");
						Sharpen.Runtime.PrintStackTrace(e, this._enclosing.err);
					}
				}
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Execute()
			{
				this.WarnIfTransientProvider();
				this._enclosing.@out.WriteLine("Deleting key: " + this.keyName + " from KeyProvider: "
					 + this.provider);
				if (this.cont)
				{
					try
					{
						this.provider.DeleteKey(this.keyName);
						this.provider.Flush();
						this._enclosing.@out.WriteLine(this.keyName + " has been successfully deleted.");
						this.PrintProviderWritten();
					}
					catch (IOException e)
					{
						this._enclosing.@out.WriteLine(this.keyName + " has not been deleted. " + e.ToString
							());
						throw;
					}
				}
			}

			public override string GetUsage()
			{
				return KeyShell.DeleteCommand.Usage + ":\n\n" + KeyShell.DeleteCommand.Desc;
			}

			private readonly KeyShell _enclosing;
		}

		private class CreateCommand : KeyShell.Command
		{
			public const string Usage = "create <keyname> [-cipher <cipher>] [-size <size>]\n"
				 + "                     [-description <description>]\n" + "                     [-attr <attribute=value>]\n"
				 + "                     [-provider <provider>] [-help]";

			public const string Desc = "The create subcommand creates a new key for the name specified\n"
				 + "by the <keyname> argument within the provider specified by the\n" + "-provider argument. You may specify a cipher with the -cipher\n"
				 + "argument. The default cipher is currently \"AES/CTR/NoPadding\".\n" + "The default keysize is 128. You may specify the requested key\n"
				 + "length using the -size argument. Arbitrary attribute=value\n" + "style attributes may be specified using the -attr argument.\n"
				 + "-attr may be specified multiple times, once per attribute.\n";

			internal readonly string keyName;

			internal readonly KeyProvider.Options options;

			public CreateCommand(KeyShell _enclosing, string keyName, KeyProvider.Options options
				)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.keyName = keyName;
				this.options = options;
			}

			public override bool Validate()
			{
				bool rc = true;
				this.provider = this.GetKeyProvider();
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
			/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
			public override void Execute()
			{
				this.WarnIfTransientProvider();
				try
				{
					this.provider.CreateKey(this.keyName, this.options);
					this.provider.Flush();
					this._enclosing.@out.WriteLine(this.keyName + " has been successfully created with options "
						 + this.options.ToString() + ".");
					this.PrintProviderWritten();
				}
				catch (InvalidParameterException e)
				{
					this._enclosing.@out.WriteLine(this.keyName + " has not been created. " + e.ToString
						());
					throw;
				}
				catch (IOException e)
				{
					this._enclosing.@out.WriteLine(this.keyName + " has not been created. " + e.ToString
						());
					throw;
				}
				catch (NoSuchAlgorithmException e)
				{
					this._enclosing.@out.WriteLine(this.keyName + " has not been created. " + e.ToString
						());
					throw;
				}
			}

			public override string GetUsage()
			{
				return KeyShell.CreateCommand.Usage + ":\n\n" + KeyShell.CreateCommand.Desc;
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
			int res = ToolRunner.Run(new Configuration(), new KeyShell(), args);
			System.Environment.Exit(res);
		}
	}
}
