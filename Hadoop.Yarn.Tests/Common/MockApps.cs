using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	/// <summary>Utilities to generate fake test apps</summary>
	public class MockApps
	{
		internal static readonly IEnumerator<string> Names = Iterators.Cycle("SleepJob", 
			"RandomWriter", "TeraSort", "TeraGen", "PigLatin", "WordCount", "I18nApp<☯>");

		internal static readonly IEnumerator<string> Users = Iterators.Cycle("dorothy", "tinman"
			, "scarecrow", "glinda", "nikko", "toto", "winkie", "zeke", "gulch");

		internal static readonly IEnumerator<YarnApplicationState> States = Iterators.Cycle
			(YarnApplicationState.Values());

		internal static readonly IEnumerator<string> Queues = Iterators.Cycle("a.a1", "a.a2"
			, "b.b1", "b.b2", "b.b3", "c.c1.c11", "c.c1.c12", "c.c1.c13", "c.c2", "c.c3", "c.c4"
			);

		internal static readonly long Ts = Runtime.CurrentTimeMillis();

		public static string NewAppName()
		{
			lock (Names)
			{
				return Names.Next();
			}
		}

		public static string NewUserName()
		{
			lock (Users)
			{
				return Users.Next();
			}
		}

		public static string NewQueue()
		{
			lock (Queues)
			{
				return Queues.Next();
			}
		}

		public static ApplicationId NewAppID(int i)
		{
			return ApplicationId.NewInstance(Ts, i);
		}

		public static YarnApplicationState NewAppState()
		{
			lock (States)
			{
				return States.Next();
			}
		}
	}
}
