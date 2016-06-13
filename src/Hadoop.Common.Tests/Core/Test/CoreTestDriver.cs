using System;
using Hadoop.Common.Tests.Core.Ipc;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Util;

namespace Hadoop.Common.Tests.Core.Test
{
	/// <summary>Driver for core tests.</summary>
	public class CoreTestDriver
	{
		private ProgramDriver pgd;

		public CoreTestDriver()
			: this(new ProgramDriver())
		{
		}

		public CoreTestDriver(ProgramDriver pgd)
		{
			this.pgd = pgd;
			try
			{
				pgd.AddClass("testsetfile", typeof(TestSetFile), "A test for flat files of binary key/value pairs."
					);
				pgd.AddClass("testarrayfile", typeof(TestArrayFile), "A test for flat files of binary key/value pairs."
					);
				pgd.AddClass("testrpc", typeof(TestRPC), "A test for rpc.");
				pgd.AddClass("testipc", typeof(TestIPC), "A test for ipc.");
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
			}
		}

		public virtual void Run(string[] argv)
		{
			int exitCode = -1;
			try
			{
				exitCode = pgd.Run(argv);
			}
			catch (Exception e)
			{
				Runtime.PrintStackTrace(e);
			}
			System.Environment.Exit(exitCode);
		}

		public static void Main(string[] argv)
		{
			new CoreTestDriver().Run(argv);
		}
	}
}
