using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	public class MapHost
	{
		public enum State
		{
			Idle,
			Busy,
			Pending,
			Penalized
		}

		private MapHost.State state = MapHost.State.Idle;

		private readonly string hostName;

		private readonly string baseUrl;

		private IList<TaskAttemptID> maps = new AList<TaskAttemptID>();

		public MapHost(string hostName, string baseUrl)
		{
			// No map outputs available
			// Map outputs are being fetched
			// Known map outputs which need to be fetched
			// Host penalized due to shuffle failures
			this.hostName = hostName;
			this.baseUrl = baseUrl;
		}

		public virtual MapHost.State GetState()
		{
			return state;
		}

		public virtual string GetHostName()
		{
			return hostName;
		}

		public virtual string GetBaseUrl()
		{
			return baseUrl;
		}

		public virtual void AddKnownMap(TaskAttemptID mapId)
		{
			lock (this)
			{
				maps.AddItem(mapId);
				if (state == MapHost.State.Idle)
				{
					state = MapHost.State.Pending;
				}
			}
		}

		public virtual IList<TaskAttemptID> GetAndClearKnownMaps()
		{
			lock (this)
			{
				IList<TaskAttemptID> currentKnownMaps = maps;
				maps = new AList<TaskAttemptID>();
				return currentKnownMaps;
			}
		}

		public virtual void MarkBusy()
		{
			lock (this)
			{
				state = MapHost.State.Busy;
			}
		}

		public virtual void MarkPenalized()
		{
			lock (this)
			{
				state = MapHost.State.Penalized;
			}
		}

		public virtual int GetNumKnownMapOutputs()
		{
			lock (this)
			{
				return maps.Count;
			}
		}

		/// <summary>Called when the node is done with its penalty or done copying.</summary>
		/// <returns>the host's new state</returns>
		public virtual MapHost.State MarkAvailable()
		{
			lock (this)
			{
				if (maps.IsEmpty())
				{
					state = MapHost.State.Idle;
				}
				else
				{
					state = MapHost.State.Pending;
				}
				return state;
			}
		}

		public override string ToString()
		{
			return hostName;
		}

		/// <summary>Mark the host as penalized</summary>
		public virtual void Penalize()
		{
			lock (this)
			{
				state = MapHost.State.Penalized;
			}
		}
	}
}
