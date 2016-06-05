

namespace Org.Apache.Hadoop.Util
{
	/// <summary>Represents an object that you can wait for.</summary>
	public class Waitable<T>
	{
		private T val;

		private readonly Condition cond;

		public Waitable(Condition cond)
		{
			this.val = null;
			this.cond = cond;
		}

		/// <exception cref="System.Exception"/>
		public virtual T Await()
		{
			while (this.val == null)
			{
				this.cond.Await();
			}
			return this.val;
		}

		public virtual void Provide(T val)
		{
			this.val = val;
			this.cond.SignalAll();
		}

		public virtual bool HasVal()
		{
			return this.val != null;
		}

		public virtual T GetVal()
		{
			return this.val;
		}
	}
}
