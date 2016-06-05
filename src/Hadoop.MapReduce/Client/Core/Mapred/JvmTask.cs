using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Task abstraction that can be serialized, implements the writable interface.
	/// 	</summary>
	public class JvmTask : Writable
	{
		internal Task t;

		internal bool shouldDie;

		public JvmTask(Task t, bool shouldDie)
		{
			this.t = t;
			this.shouldDie = shouldDie;
		}

		public JvmTask()
		{
		}

		public virtual Task GetTask()
		{
			return t;
		}

		public virtual bool ShouldDie()
		{
			return shouldDie;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteBoolean(shouldDie);
			if (t != null)
			{
				@out.WriteBoolean(true);
				@out.WriteBoolean(t.IsMapTask());
				t.Write(@out);
			}
			else
			{
				@out.WriteBoolean(false);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			shouldDie = @in.ReadBoolean();
			bool taskComing = @in.ReadBoolean();
			if (taskComing)
			{
				bool isMap = @in.ReadBoolean();
				if (isMap)
				{
					t = new MapTask();
				}
				else
				{
					t = new ReduceTask();
				}
				t.ReadFields(@in);
			}
		}
	}
}
