using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>Represents an object that you can wait for.</summary>
	public class Waitable<T>
	{
		private T val;

		private readonly java.util.concurrent.locks.Condition cond;

		public Waitable(java.util.concurrent.locks.Condition cond)
		{
			this.val = null;
			this.cond = cond;
		}

		/// <exception cref="System.Exception"/>
		public virtual T await()
		{
			while (this.val == null)
			{
				this.cond.await();
			}
			return this.val;
		}

		public virtual void provide(T val)
		{
			this.val = val;
			this.cond.signalAll();
		}

		public virtual bool hasVal()
		{
			return this.val != null;
		}

		public virtual T getVal()
		{
			return this.val;
		}
	}
}
