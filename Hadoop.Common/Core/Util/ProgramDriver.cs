using System;
using System.Collections.Generic;
using System.Reflection;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>A driver that is used to run programs added to it</summary>
	public class ProgramDriver
	{
		/// <summary>
		/// A description of a program based on its class and a
		/// human-readable description.
		/// </summary>
		internal IDictionary<string, ProgramDriver.ProgramDescription> programs;

		public ProgramDriver()
		{
			programs = new SortedDictionary<string, ProgramDriver.ProgramDescription>();
		}

		private class ProgramDescription
		{
			internal static readonly Type[] paramTypes = new Type[] { typeof(string[]) };

			/// <summary>Create a description of an example program.</summary>
			/// <param name="mainClass">the class with the main for the example program</param>
			/// <param name="description">a string to display to the user in help messages</param>
			/// <exception cref="System.Security.SecurityException">if we can't use reflection</exception>
			/// <exception cref="System.MissingMethodException">if the class doesn't have a main method
			/// 	</exception>
			public ProgramDescription(Type mainClass, string description)
			{
				this.main = mainClass.GetMethod("main", paramTypes);
				this.description = description;
			}

			/// <summary>Invoke the example application with the given arguments</summary>
			/// <param name="args">the arguments for the application</param>
			/// <exception cref="System.Exception">The exception thrown by the invoked method</exception>
			public virtual void Invoke(string[] args)
			{
				try
				{
					main.Invoke(null, new object[] { args });
				}
				catch (TargetInvocationException except)
				{
					throw except.InnerException;
				}
			}

			public virtual string GetDescription()
			{
				return description;
			}

			private MethodInfo main;

			private string description;
		}

		private static void PrintUsage(IDictionary<string, ProgramDriver.ProgramDescription
			> programs)
		{
			System.Console.Out.WriteLine("Valid program names are:");
			foreach (KeyValuePair<string, ProgramDriver.ProgramDescription> item in programs)
			{
				System.Console.Out.WriteLine("  " + item.Key + ": " + item.Value.GetDescription()
					);
			}
		}

		/// <summary>This is the method that adds the classed to the repository</summary>
		/// <param name="name">The name of the string you want the class instance to be called with
		/// 	</param>
		/// <param name="mainClass">The class that you want to add to the repository</param>
		/// <param name="description">The description of the class</param>
		/// <exception cref="System.MissingMethodException"></exception>
		/// <exception cref="System.Security.SecurityException"></exception>
		/// <exception cref="System.Exception"/>
		public virtual void AddClass(string name, Type mainClass, string description)
		{
			programs[name] = new ProgramDriver.ProgramDescription(mainClass, description);
		}

		/// <summary>This is a driver for the example programs.</summary>
		/// <remarks>
		/// This is a driver for the example programs.
		/// It looks at the first command line argument and tries to find an
		/// example program with that name.
		/// If it is found, it calls the main method in that class with the rest
		/// of the command line arguments.
		/// </remarks>
		/// <param name="args">The argument from the user. args[0] is the command to run.</param>
		/// <returns>-1 on error, 0 on success</returns>
		/// <exception cref="System.MissingMethodException"></exception>
		/// <exception cref="System.Security.SecurityException"></exception>
		/// <exception cref="System.MemberAccessException"></exception>
		/// <exception cref="System.ArgumentException"></exception>
		/// <exception cref="System.Exception">Anything thrown by the example program's main</exception>
		public virtual int Run(string[] args)
		{
			// Make sure they gave us a program name.
			if (args.Length == 0)
			{
				System.Console.Out.WriteLine("An example program must be given as the" + " first argument."
					);
				PrintUsage(programs);
				return -1;
			}
			// And that it is good.
			ProgramDriver.ProgramDescription pgm = programs[args[0]];
			if (pgm == null)
			{
				System.Console.Out.WriteLine("Unknown program '" + args[0] + "' chosen.");
				PrintUsage(programs);
				return -1;
			}
			// Remove the leading argument and call main
			string[] new_args = new string[args.Length - 1];
			for (int i = 1; i < args.Length; ++i)
			{
				new_args[i - 1] = args[i];
			}
			pgm.Invoke(new_args);
			return 0;
		}

		/// <summary>API compatible with Hadoop 1.x</summary>
		/// <exception cref="System.Exception"/>
		public virtual void Driver(string[] argv)
		{
			if (Run(argv) == -1)
			{
				System.Environment.Exit(-1);
			}
		}
	}
}
