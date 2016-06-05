using System;
using System.IO;
using Com.Google.Common.Collect;
using Com.Google.Common.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Tools
{
	public class TestTools
	{
		private const int PipeBufferSize = 1024 * 5;

		private const string InvalidOption = "-invalidOption";

		private static readonly string[] Options = new string[2];

		[BeforeClass]
		public static void Before()
		{
			ExitUtil.DisableSystemExit();
			Options[1] = InvalidOption;
		}

		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenFetcherPrintUsage()
		{
			string pattern = "Options:";
			CheckOutput(new string[] { "-help" }, pattern, System.Console.Out, typeof(DelegationTokenFetcher
				));
		}

		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenFetcherErrorOption()
		{
			string pattern = "ERROR: Only specify cancel, renew or print.";
			CheckOutput(new string[] { "-cancel", "-renew" }, pattern, System.Console.Error, 
				typeof(DelegationTokenFetcher));
		}

		[NUnit.Framework.Test]
		public virtual void TestJMXToolHelp()
		{
			string pattern = "usage: jmxget options are:";
			CheckOutput(new string[] { "-help" }, pattern, System.Console.Out, typeof(JMXGet)
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestJMXToolAdditionParameter()
		{
			string pattern = "key = -addition";
			CheckOutput(new string[] { "-service=NameNode", "-server=localhost", "-addition" }
				, pattern, System.Console.Error, typeof(JMXGet));
		}

		[NUnit.Framework.Test]
		public virtual void TestDFSAdminInvalidUsageHelp()
		{
			ImmutableSet<string> args = ImmutableSet.Of("-report", "-saveNamespace", "-rollEdits"
				, "-restoreFailedStorage", "-refreshNodes", "-finalizeUpgrade", "-metasave", "-refreshUserToGroupsMappings"
				, "-printTopology", "-refreshNamenodes", "-deleteBlockPool", "-setBalancerBandwidth"
				, "-fetchImage");
			try
			{
				foreach (string arg in args)
				{
					NUnit.Framework.Assert.IsTrue(ToolRunner.Run(new DFSAdmin(), FillArgs(arg)) == -1
						);
				}
				NUnit.Framework.Assert.IsTrue(ToolRunner.Run(new DFSAdmin(), new string[] { "-help"
					, "-some" }) == 0);
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("testDFSAdminHelp error" + e);
			}
			string pattern = "Usage: hdfs dfsadmin";
			CheckOutput(new string[] { "-cancel", "-renew" }, pattern, System.Console.Error, 
				typeof(DFSAdmin));
		}

		private static string[] FillArgs(string arg)
		{
			Options[0] = arg;
			return Options;
		}

		private void CheckOutput(string[] args, string pattern, TextWriter @out, Type clazz
			)
		{
			ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
			try
			{
				PipedOutputStream pipeOut = new PipedOutputStream();
				PipedInputStream pipeIn = new PipedInputStream(pipeOut, PipeBufferSize);
				if (@out == System.Console.Out)
				{
					Runtime.SetOut(new TextWriter(pipeOut));
				}
				else
				{
					if (@out == System.Console.Error)
					{
						Runtime.SetErr(new TextWriter(pipeOut));
					}
				}
				if (clazz == typeof(DelegationTokenFetcher))
				{
					ExpectDelegationTokenFetcherExit(args);
				}
				else
				{
					if (clazz == typeof(JMXGet))
					{
						ExpectJMXGetExit(args);
					}
					else
					{
						if (clazz == typeof(DFSAdmin))
						{
							ExpectDfsAdminPrint(args);
						}
					}
				}
				pipeOut.Close();
				ByteStreams.Copy(pipeIn, outBytes);
				pipeIn.Close();
				NUnit.Framework.Assert.IsTrue(Sharpen.Runtime.GetStringForBytes(outBytes.ToByteArray
					()).Contains(pattern));
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.Fail("checkOutput error " + ex);
			}
		}

		private void ExpectDfsAdminPrint(string[] args)
		{
			try
			{
				ToolRunner.Run(new DFSAdmin(), args);
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.Fail("expectDelegationTokenFetcherExit ex error " + ex);
			}
		}

		private static void ExpectDelegationTokenFetcherExit(string[] args)
		{
			try
			{
				DelegationTokenFetcher.Main(args);
				NUnit.Framework.Assert.Fail("should call exit");
			}
			catch (ExitUtil.ExitException)
			{
				ExitUtil.ResetFirstExitException();
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.Fail("expectDelegationTokenFetcherExit ex error " + ex);
			}
		}

		private static void ExpectJMXGetExit(string[] args)
		{
			try
			{
				JMXGet.Main(args);
				NUnit.Framework.Assert.Fail("should call exit");
			}
			catch (ExitUtil.ExitException)
			{
				ExitUtil.ResetFirstExitException();
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.Fail("expectJMXGetExit ex error " + ex);
			}
		}
	}
}
