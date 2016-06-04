using System;
using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	/// <summary>Parse the args of a command and check the format of args.</summary>
	public class CommandFormat
	{
		internal readonly int minPar;

		internal readonly int maxPar;

		internal readonly IDictionary<string, bool> options = new Dictionary<string, bool
			>();

		internal bool ignoreUnknownOpts = false;

		/// <param name="name">of command, but never used</param>
		/// <param name="min">see replacement</param>
		/// <param name="max">see replacement</param>
		/// <param name="possibleOpt">see replacement</param>
		/// <seealso cref="CommandFormat(int, int, string[])"/>
		[System.ObsoleteAttribute(@"use replacement since name is an unused parameter")]
		public CommandFormat(string n, int min, int max, params string[] possibleOpt)
			: this(min, max, possibleOpt)
		{
		}

		/// <summary>Simple parsing of command line arguments</summary>
		/// <param name="min">minimum arguments required</param>
		/// <param name="max">maximum arguments permitted</param>
		/// <param name="possibleOpt">list of the allowed switches</param>
		public CommandFormat(int min, int max, params string[] possibleOpt)
		{
			minPar = min;
			maxPar = max;
			foreach (string opt in possibleOpt)
			{
				if (opt == null)
				{
					ignoreUnknownOpts = true;
				}
				else
				{
					options[opt] = false;
				}
			}
		}

		/// <summary>
		/// Parse parameters starting from the given position
		/// Consider using the variant that directly takes a List
		/// </summary>
		/// <param name="args">an array of input arguments</param>
		/// <param name="pos">the position at which starts to parse</param>
		/// <returns>a list of parameters</returns>
		public virtual IList<string> Parse(string[] args, int pos)
		{
			IList<string> parameters = new AList<string>(Arrays.AsList(args));
			parameters.SubList(0, pos).Clear();
			Parse(parameters);
			return parameters;
		}

	    /// <summary>Parse parameters from the given list of args.</summary>
	    /// <remarks>
	    /// Parse parameters from the given list of args.  The list is
	    /// destructively modified to remove the options.
	    /// </remarks>
	    /// <param name="args">as a list of input arguments</param>
	    public virtual void Parse(List<string> args)
		{
			int pos = 0;
			while (pos < args.Count)
			{
				string arg = args[pos];
				// stop if not an opt, or the stdin arg "-" is found
				if (!arg.StartsWith("-") || arg.Equals("-"))
				{
					break;
				}
				else
				{
					if (arg.Equals("--"))
					{
						// force end of option processing
						args.Remove(pos);
						break;
					}
				}
				string opt = Sharpen.Runtime.Substring(arg, 1);
				if (options.Contains(opt))
				{
					args.Remove(pos);
					options[opt] = true;
				}
				else
				{
					if (ignoreUnknownOpts)
					{
						pos++;
					}
					else
					{
						throw new CommandFormat.UnknownOptionException(arg);
					}
				}
			}
			int psize = args.Count;
			if (psize < minPar)
			{
				throw new CommandFormat.NotEnoughArgumentsException(minPar, psize);
			}
			if (psize > maxPar)
			{
				throw new CommandFormat.TooManyArgumentsException(maxPar, psize);
			}
		}

		/// <summary>Return if the option is set or not</summary>
		/// <param name="option">String representation of an option</param>
		/// <returns>true is the option is set; false otherwise</returns>
		public virtual bool GetOpt(string option)
		{
			return options.Contains(option) ? options[option] : false;
		}

		/// <summary>Returns all the options that are set</summary>
		/// <returns>Set<String> of the enabled options</returns>
		public virtual ICollection<string> GetOpts()
		{
			ICollection<string> optSet = new HashSet<string>();
			foreach (KeyValuePair<string, bool> entry in options)
			{
				if (entry.Value)
				{
					optSet.AddItem(entry.Key);
				}
			}
			return optSet;
		}

		/// <summary>Used when the arguments exceed their bounds</summary>
		[System.Serializable]
		public abstract class IllegalNumberOfArgumentsException : ArgumentException
		{
			private const long serialVersionUID = 0L;

			protected internal int expected;

			protected internal int actual;

			protected internal IllegalNumberOfArgumentsException(int want, int got)
			{
				expected = want;
				actual = got;
			}

			public override string Message
			{
				get
				{
					return "expected " + expected + " but got " + actual;
				}
			}
		}

		/// <summary>Used when too many arguments are supplied to a command</summary>
		[System.Serializable]
		public class TooManyArgumentsException : CommandFormat.IllegalNumberOfArgumentsException
		{
			private const long serialVersionUID = 0L;

			public TooManyArgumentsException(int expected, int actual)
				: base(expected, actual)
			{
			}

			public override string Message
			{
				get
				{
					return "Too many arguments: " + base.Message;
				}
			}
		}

		/// <summary>Used when too few arguments are supplied to a command</summary>
		[System.Serializable]
		public class NotEnoughArgumentsException : CommandFormat.IllegalNumberOfArgumentsException
		{
			private const long serialVersionUID = 0L;

			public NotEnoughArgumentsException(int expected, int actual)
				: base(expected, actual)
			{
			}

			public override string Message
			{
				get
				{
					return "Not enough arguments: " + base.Message;
				}
			}
		}

		/// <summary>Used when an unsupported option is supplied to a command</summary>
		[System.Serializable]
		public class UnknownOptionException : ArgumentException
		{
			private const long serialVersionUID = 0L;

			protected internal string option = null;

			public UnknownOptionException(string unknownOption)
				: base("Illegal option " + unknownOption)
			{
				option = unknownOption;
			}

			public virtual string GetOption()
			{
				return option;
			}
		}
	}
}
