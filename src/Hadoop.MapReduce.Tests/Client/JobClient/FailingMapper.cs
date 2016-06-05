using System;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop
{
	/// <summary>Fails the Mapper.</summary>
	/// <remarks>Fails the Mapper. First attempt throws exception. Rest do System.exit.</remarks>
	public class FailingMapper : Mapper<Text, Text, Text, Text>
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected override void Map(Text key, Text value, Mapper.Context context)
		{
			// Just create a non-daemon thread which hangs forever. MR AM should not be
			// hung by this.
			new _Thread_36().Start();
			//
			if (context.GetTaskAttemptID().GetId() == 0)
			{
				System.Console.Out.WriteLine("Attempt:" + context.GetTaskAttemptID() + " Failing mapper throwing exception"
					);
				throw new IOException("Attempt:" + context.GetTaskAttemptID() + " Failing mapper throwing exception"
					);
			}
			else
			{
				System.Console.Out.WriteLine("Attempt:" + context.GetTaskAttemptID() + " Exiting"
					);
				System.Environment.Exit(-1);
			}
		}

		private sealed class _Thread_36 : Sharpen.Thread
		{
			public _Thread_36()
			{
			}

			public override void Run()
			{
				lock (this)
				{
					try
					{
						Sharpen.Runtime.Wait(this);
					}
					catch (Exception)
					{
					}
				}
			}
		}
	}
}
