using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// <code>JobQueueClient</code> is interface provided to the user in order to get
	/// JobQueue related information from the
	/// <see cref="JobTracker"/>
	/// It provides the facility to list the JobQueues present and ability to view
	/// the list of jobs within a specific JobQueue
	/// </summary>
	internal class JobQueueClient : Configured, Tool
	{
		internal JobClient jc;

		public JobQueueClient()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public JobQueueClient(JobConf conf)
		{
			SetConf(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private void Init(JobConf conf)
		{
			SetConf(conf);
			jc = new JobClient(conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] argv)
		{
			int exitcode = -1;
			if (argv.Length < 1)
			{
				DisplayUsage(string.Empty);
				return exitcode;
			}
			string cmd = argv[0];
			bool displayQueueList = false;
			bool displayQueueInfoWithJobs = false;
			bool displayQueueInfoWithoutJobs = false;
			bool displayQueueAclsInfoForCurrentUser = false;
			if ("-list".Equals(cmd))
			{
				displayQueueList = true;
			}
			else
			{
				if ("-showacls".Equals(cmd))
				{
					displayQueueAclsInfoForCurrentUser = true;
				}
				else
				{
					if ("-info".Equals(cmd))
					{
						if (argv.Length == 2 && !(argv[1].Equals("-showJobs")))
						{
							displayQueueInfoWithoutJobs = true;
						}
						else
						{
							if (argv.Length == 3)
							{
								if (argv[2].Equals("-showJobs"))
								{
									displayQueueInfoWithJobs = true;
								}
								else
								{
									DisplayUsage(cmd);
									return exitcode;
								}
							}
							else
							{
								DisplayUsage(cmd);
								return exitcode;
							}
						}
					}
					else
					{
						DisplayUsage(cmd);
						return exitcode;
					}
				}
			}
			JobConf conf = new JobConf(GetConf());
			Init(conf);
			if (displayQueueList)
			{
				DisplayQueueList();
				exitcode = 0;
			}
			else
			{
				if (displayQueueInfoWithoutJobs)
				{
					DisplayQueueInfo(argv[1], false);
					exitcode = 0;
				}
				else
				{
					if (displayQueueInfoWithJobs)
					{
						DisplayQueueInfo(argv[1], true);
						exitcode = 0;
					}
					else
					{
						if (displayQueueAclsInfoForCurrentUser)
						{
							this.DisplayQueueAclsInfoForCurrentUser();
							exitcode = 0;
						}
					}
				}
			}
			return exitcode;
		}

		// format and print information about the passed in job queue.
		/// <exception cref="System.IO.IOException"/>
		internal virtual void PrintJobQueueInfo(JobQueueInfo jobQueueInfo, TextWriter writer
			)
		{
			PrintJobQueueInfo(jobQueueInfo, writer, string.Empty);
		}

		// format and print information about the passed in job queue.
		/// <exception cref="System.IO.IOException"/>
		internal virtual void PrintJobQueueInfo(JobQueueInfo jobQueueInfo, TextWriter writer
			, string prefix)
		{
			if (jobQueueInfo == null)
			{
				writer.Write("No queue found.\n");
				writer.Flush();
				return;
			}
			writer.Write(string.Format(prefix + "======================\n"));
			writer.Write(string.Format(prefix + "Queue Name : %s \n", jobQueueInfo.GetQueueName
				()));
			writer.Write(string.Format(prefix + "Queue State : %s \n", jobQueueInfo.GetQueueState
				()));
			writer.Write(string.Format(prefix + "Scheduling Info : %s \n", jobQueueInfo.GetSchedulingInfo
				()));
			IList<JobQueueInfo> childQueues = jobQueueInfo.GetChildren();
			if (childQueues != null && childQueues.Count > 0)
			{
				for (int i = 0; i < childQueues.Count; i++)
				{
					PrintJobQueueInfo(childQueues[i], writer, "    " + prefix);
				}
			}
			writer.Flush();
		}

		/// <exception cref="System.IO.IOException"/>
		private void DisplayQueueList()
		{
			JobQueueInfo[] rootQueues = jc.GetRootQueues();
			foreach (JobQueueInfo queue in rootQueues)
			{
				PrintJobQueueInfo(queue, new PrintWriter(new OutputStreamWriter(System.Console.Out
					, Charsets.Utf8)));
			}
		}

		/// <summary>
		/// Expands the hierarchy of queues and gives the list of all queues in
		/// depth-first order
		/// </summary>
		/// <param name="rootQueues">the top-level queues</param>
		/// <returns>the list of all the queues in depth-first order.</returns>
		internal virtual IList<JobQueueInfo> ExpandQueueList(JobQueueInfo[] rootQueues)
		{
			IList<JobQueueInfo> allQueues = new AList<JobQueueInfo>();
			foreach (JobQueueInfo queue in rootQueues)
			{
				allQueues.AddItem(queue);
				if (queue.GetChildren() != null)
				{
					JobQueueInfo[] childQueues = Sharpen.Collections.ToArray(queue.GetChildren(), new 
						JobQueueInfo[0]);
					Sharpen.Collections.AddAll(allQueues, ExpandQueueList(childQueues));
				}
			}
			return allQueues;
		}

		/// <summary>
		/// Method used to display information pertaining to a Single JobQueue
		/// registered with the
		/// <see cref="QueueManager"/>
		/// . Display of the Jobs is determine
		/// by the boolean
		/// </summary>
		/// <exception cref="System.IO.IOException">, InterruptedException</exception>
		/// <exception cref="System.Exception"/>
		private void DisplayQueueInfo(string queue, bool showJobs)
		{
			JobQueueInfo jobQueueInfo = jc.GetQueueInfo(queue);
			if (jobQueueInfo == null)
			{
				System.Console.Out.WriteLine("Queue \"" + queue + "\" does not exist.");
				return;
			}
			PrintJobQueueInfo(jobQueueInfo, new PrintWriter(new OutputStreamWriter(System.Console.Out
				, Charsets.Utf8)));
			if (showJobs && (jobQueueInfo.GetChildren() == null || jobQueueInfo.GetChildren()
				.Count == 0))
			{
				JobStatus[] jobs = jobQueueInfo.GetJobStatuses();
				if (jobs == null)
				{
					jobs = new JobStatus[0];
				}
				jc.DisplayJobList(jobs);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DisplayQueueAclsInfoForCurrentUser()
		{
			QueueAclsInfo[] queueAclsInfoList = jc.GetQueueAclsForCurrentUser();
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			if (queueAclsInfoList.Length > 0)
			{
				System.Console.Out.WriteLine("Queue acls for user :  " + ugi.GetShortUserName());
				System.Console.Out.WriteLine("\nQueue  Operations");
				System.Console.Out.WriteLine("=====================");
				foreach (QueueAclsInfo queueInfo in queueAclsInfoList)
				{
					System.Console.Out.Write(queueInfo.GetQueueName() + "  ");
					string[] ops = queueInfo.GetOperations();
					Arrays.Sort(ops);
					int max = ops.Length - 1;
					for (int j = 0; j < ops.Length; j++)
					{
						System.Console.Out.Write(ops[j].ReplaceFirst("acl-", string.Empty));
						if (j < max)
						{
							System.Console.Out.Write(",");
						}
					}
					System.Console.Out.WriteLine();
				}
			}
			else
			{
				System.Console.Out.WriteLine("User " + ugi.GetShortUserName() + " does not have access to any queue. \n"
					);
			}
		}

		private void DisplayUsage(string cmd)
		{
			string prefix = "Usage: JobQueueClient ";
			if ("-queueinfo".Equals(cmd))
			{
				System.Console.Error.WriteLine(prefix + "[" + cmd + "<job-queue-name> [-showJobs]]"
					);
			}
			else
			{
				System.Console.Error.Printf(prefix + "<command> <args>%n");
				System.Console.Error.Printf("\t[-list]%n");
				System.Console.Error.Printf("\t[-info <job-queue-name> [-showJobs]]%n");
				System.Console.Error.Printf("\t[-showacls] %n%n");
				ToolRunner.PrintGenericCommandUsage(System.Console.Out);
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			int res = ToolRunner.Run(new Org.Apache.Hadoop.Mapred.JobQueueClient(), argv);
			System.Environment.Exit(res);
		}
	}
}
