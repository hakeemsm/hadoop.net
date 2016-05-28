using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>Enum representing queue state</summary>
	[System.Serializable]
	public sealed class QueueState
	{
		public static readonly Org.Apache.Hadoop.Mapreduce.QueueState Stopped = new Org.Apache.Hadoop.Mapreduce.QueueState
			("stopped");

		public static readonly Org.Apache.Hadoop.Mapreduce.QueueState Running = new Org.Apache.Hadoop.Mapreduce.QueueState
			("running");

		public static readonly Org.Apache.Hadoop.Mapreduce.QueueState Undefined = new Org.Apache.Hadoop.Mapreduce.QueueState
			("undefined");

		private readonly string stateName;

		private static IDictionary<string, Org.Apache.Hadoop.Mapreduce.QueueState> enumMap
			 = new Dictionary<string, Org.Apache.Hadoop.Mapreduce.QueueState>();

		static QueueState()
		{
			foreach (Org.Apache.Hadoop.Mapreduce.QueueState state in Org.Apache.Hadoop.Mapreduce.QueueState
				.Values())
			{
				Org.Apache.Hadoop.Mapreduce.QueueState.enumMap[state.GetStateName()] = state;
			}
		}

		internal QueueState(string stateName)
		{
			this.stateName = stateName;
		}

		/// <returns>the stateName</returns>
		public string GetStateName()
		{
			return Org.Apache.Hadoop.Mapreduce.QueueState.stateName;
		}

		public static Org.Apache.Hadoop.Mapreduce.QueueState GetState(string state)
		{
			Org.Apache.Hadoop.Mapreduce.QueueState qState = Org.Apache.Hadoop.Mapreduce.QueueState
				.enumMap[state];
			if (qState == null)
			{
				return Org.Apache.Hadoop.Mapreduce.QueueState.Undefined;
			}
			return qState;
		}

		public override string ToString()
		{
			return Org.Apache.Hadoop.Mapreduce.QueueState.stateName;
		}
	}
}
