using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Context data for an ongoing NameNode metadata recovery process.</summary>
	public sealed class MetaRecoveryContext
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.MetaRecoveryContext
			).FullName);

		public const int ForceNone = 0;

		public const int ForceFirstChoice = 1;

		public const int ForceAll = 2;

		private int force;

		/// <summary>Exception thrown when the user has requested processing to stop.</summary>
		[System.Serializable]
		public class RequestStopException : IOException
		{
			private const long serialVersionUID = 1L;

			public RequestStopException(string msg)
				: base(msg)
			{
			}
		}

		public MetaRecoveryContext(int force)
		{
			this.force = force;
		}

		/// <summary>Display a prompt to the user and get his or her choice.</summary>
		/// <param name="prompt">The prompt to display</param>
		/// <?/>
		/// <param name="choices">Other choies</param>
		/// <returns>The choice that was taken</returns>
		/// <exception cref="System.IO.IOException"/>
		public string Ask(string prompt, string firstChoice, params string[] choices)
		{
			while (true)
			{
				System.Console.Error.Write(prompt);
				if (force > ForceNone)
				{
					System.Console.Out.WriteLine("automatically choosing " + firstChoice);
					return firstChoice;
				}
				StringBuilder responseBuilder = new StringBuilder();
				while (true)
				{
					int c = Runtime.@in.Read();
					if (c == -1 || c == '\r' || c == '\n')
					{
						break;
					}
					responseBuilder.Append((char)c);
				}
				string response = responseBuilder.ToString();
				if (Sharpen.Runtime.EqualsIgnoreCase(response, firstChoice))
				{
					return firstChoice;
				}
				foreach (string c_1 in choices)
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(response, c_1))
					{
						return c_1;
					}
				}
				System.Console.Error.Write("I'm sorry, I cannot understand your response.\n");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.MetaRecoveryContext.RequestStopException
		/// 	"/>
		public static void EditLogLoaderPrompt(string prompt, MetaRecoveryContext recovery
			, string contStr)
		{
			if (recovery == null)
			{
				throw new IOException(prompt);
			}
			Log.Error(prompt);
			string answer = recovery.Ask("\nEnter 'c' to continue, " + contStr + "\n" + "Enter 's' to stop reading the edit log here, abandoning any later "
				 + "edits\n" + "Enter 'q' to quit without saving\n" + "Enter 'a' to always select the first choice in the future "
				 + "without prompting. " + "(c/s/q/a)\n", "c", "s", "q", "a");
			if (answer.Equals("c"))
			{
				Log.Info("Continuing");
				return;
			}
			else
			{
				if (answer.Equals("s"))
				{
					throw new MetaRecoveryContext.RequestStopException("user requested stop");
				}
				else
				{
					if (answer.Equals("q"))
					{
						recovery.Quit();
					}
					else
					{
						recovery.SetForce(ForceFirstChoice);
						return;
					}
				}
			}
		}

		/// <summary>Log a message and quit</summary>
		public void Quit()
		{
			Log.Error("Exiting on user request.");
			System.Environment.Exit(0);
		}

		public int GetForce()
		{
			return this.force;
		}

		public void SetForce(int force)
		{
			this.force = force;
		}
	}
}
