using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Examples.PI.Math;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI
{
	/// <summary>A Writable class for Summation</summary>
	public sealed class SummationWritable : WritableComparable<Org.Apache.Hadoop.Examples.PI.SummationWritable
		>, Container<Summation>
	{
		private Summation sigma;

		public SummationWritable()
		{
		}

		internal SummationWritable(Summation sigma)
		{
			this.sigma = sigma;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override string ToString()
		{
			return GetType().Name + sigma;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public Summation GetElement()
		{
			return sigma;
		}

		/// <summary>Read sigma from conf</summary>
		public static Summation Read(Type clazz, Configuration conf)
		{
			return Summation.ValueOf(conf.Get(clazz.Name + ".sigma"));
		}

		/// <summary>Write sigma to conf</summary>
		public static void Write(Summation sigma, Type clazz, Configuration conf)
		{
			conf.Set(clazz.Name + ".sigma", sigma.ToString());
		}

		/// <summary>Read Summation from DataInput</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static Summation Read(DataInput @in)
		{
			Org.Apache.Hadoop.Examples.PI.SummationWritable s = new Org.Apache.Hadoop.Examples.PI.SummationWritable
				();
			s.ReadFields(@in);
			return s.GetElement();
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public void ReadFields(DataInput @in)
		{
			ArithmeticProgression N = SummationWritable.ArithmeticProgressionWritable.Read(@in
				);
			ArithmeticProgression E = SummationWritable.ArithmeticProgressionWritable.Read(@in
				);
			sigma = new Summation(N, E);
			if (@in.ReadBoolean())
			{
				sigma.SetValue(@in.ReadDouble());
			}
		}

		/// <summary>Write sigma to DataOutput</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void Write(Summation sigma, DataOutput @out)
		{
			SummationWritable.ArithmeticProgressionWritable.Write(sigma.N, @out);
			SummationWritable.ArithmeticProgressionWritable.Write(sigma.E, @out);
			double v = sigma.GetValue();
			if (v == null)
			{
				@out.WriteBoolean(false);
			}
			else
			{
				@out.WriteBoolean(true);
				@out.WriteDouble(v);
			}
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public void Write(DataOutput @out)
		{
			Write(sigma, @out);
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public int CompareTo(Org.Apache.Hadoop.Examples.PI.SummationWritable that)
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
				if (obj != null && obj is Org.Apache.Hadoop.Examples.PI.SummationWritable)
				{
					Org.Apache.Hadoop.Examples.PI.SummationWritable that = (Org.Apache.Hadoop.Examples.PI.SummationWritable
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

		/// <summary>A writable class for ArithmeticProgression</summary>
		private class ArithmeticProgressionWritable
		{
			/// <summary>Read ArithmeticProgression from DataInput</summary>
			/// <exception cref="System.IO.IOException"/>
			private static ArithmeticProgression Read(DataInput @in)
			{
				return new ArithmeticProgression(@in.ReadChar(), @in.ReadLong(), @in.ReadLong(), 
					@in.ReadLong());
			}

			/// <summary>Write ArithmeticProgression to DataOutput</summary>
			/// <exception cref="System.IO.IOException"/>
			private static void Write(ArithmeticProgression ap, DataOutput @out)
			{
				@out.WriteChar(ap.symbol);
				@out.WriteLong(ap.value);
				@out.WriteLong(ap.delta);
				@out.WriteLong(ap.limit);
			}
		}
	}
}
