using System;
using System.IO;
using Org.Apache.Hadoop.Examples.PI.Math;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI
{
	/// <summary>A class for map task results or reduce task results.</summary>
	public class TaskResult : Container<Summation>, Combinable<Org.Apache.Hadoop.Examples.PI.TaskResult
		>, Writable
	{
		private Summation sigma;

		private long duration;

		public TaskResult()
		{
		}

		internal TaskResult(Summation sigma, long duration)
		{
			this.sigma = sigma;
			this.duration = duration;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public virtual Summation GetElement()
		{
			return sigma;
		}

		/// <returns>The time duration used</returns>
		internal virtual long GetDuration()
		{
			return duration;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public virtual int CompareTo(Org.Apache.Hadoop.Examples.PI.TaskResult that)
		{
			return this.sigma.CompareTo(that.sigma);
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			else
			{
				if (obj != null && obj is Org.Apache.Hadoop.Examples.PI.TaskResult)
				{
					Org.Apache.Hadoop.Examples.PI.TaskResult that = (Org.Apache.Hadoop.Examples.PI.TaskResult
						)obj;
					return this.CompareTo(that) == 0;
				}
			}
			throw new ArgumentException(obj == null ? "obj == null" : "obj.getClass()=" + obj
				.GetType());
		}

		/// <summary>Not supported</summary>
		public override int GetHashCode()
		{
			throw new NotSupportedException();
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public virtual Org.Apache.Hadoop.Examples.PI.TaskResult Combine(Org.Apache.Hadoop.Examples.PI.TaskResult
			 that)
		{
			Summation s = sigma.Combine(that.sigma);
			return s == null ? null : new Org.Apache.Hadoop.Examples.PI.TaskResult(s, this.duration
				 + that.duration);
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			sigma = SummationWritable.Read(@in);
			duration = @in.ReadLong();
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			SummationWritable.Write(sigma, @out);
			@out.WriteLong(duration);
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override string ToString()
		{
			return "sigma=" + sigma + ", duration=" + duration + "(" + Util.Millis2String(duration
				) + ")";
		}

		/// <summary>Covert a String to a TaskResult</summary>
		public static Org.Apache.Hadoop.Examples.PI.TaskResult ValueOf(string s)
		{
			int i = 0;
			int j = s.IndexOf(", duration=");
			if (j < 0)
			{
				throw new ArgumentException("i=" + i + ", j=" + j + " < 0, s=" + s);
			}
			Summation sigma = Summation.ValueOf(Util.ParseStringVariable("sigma", Sharpen.Runtime.Substring
				(s, i, j)));
			i = j + 2;
			j = s.IndexOf("(", i);
			if (j < 0)
			{
				throw new ArgumentException("i=" + i + ", j=" + j + " < 0, s=" + s);
			}
			long duration = Util.ParseLongVariable("duration", Sharpen.Runtime.Substring(s, i
				, j));
			return new Org.Apache.Hadoop.Examples.PI.TaskResult(sigma, duration);
		}
	}
}
