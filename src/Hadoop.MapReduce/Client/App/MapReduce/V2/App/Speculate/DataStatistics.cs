using System;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Speculate
{
	public class DataStatistics
	{
		private int count = 0;

		private double sum = 0;

		private double sumSquares = 0;

		public DataStatistics()
		{
		}

		public DataStatistics(double initNum)
		{
			this.count = 1;
			this.sum = initNum;
			this.sumSquares = initNum * initNum;
		}

		public virtual void Add(double newNum)
		{
			lock (this)
			{
				this.count++;
				this.sum += newNum;
				this.sumSquares += newNum * newNum;
			}
		}

		public virtual void UpdateStatistics(double old, double update)
		{
			lock (this)
			{
				this.sum += update - old;
				this.sumSquares += (update * update) - (old * old);
			}
		}

		public virtual double Mean()
		{
			lock (this)
			{
				return count == 0 ? 0.0 : sum / count;
			}
		}

		public virtual double Var()
		{
			lock (this)
			{
				// E(X^2) - E(X)^2
				if (count <= 1)
				{
					return 0.0;
				}
				double mean = Mean();
				return Math.Max((sumSquares / count) - mean * mean, 0.0d);
			}
		}

		public virtual double Std()
		{
			lock (this)
			{
				return Math.Sqrt(this.Var());
			}
		}

		public virtual double Outlier(float sigma)
		{
			lock (this)
			{
				if (count != 0.0)
				{
					return Mean() + Std() * sigma;
				}
				return 0.0;
			}
		}

		public virtual double Count()
		{
			lock (this)
			{
				return count;
			}
		}

		public override string ToString()
		{
			return "DataStatistics: count is " + count + ", sum is " + sum + ", sumSquares is "
				 + sumSquares + " mean is " + Mean() + " std() is " + Std();
		}
	}
}
