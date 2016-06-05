using System;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Terasort
{
	/// <summary>
	/// An unsigned 16 byte integer class that supports addition, multiplication,
	/// and left shifts.
	/// </summary>
	internal class Unsigned16 : Writable
	{
		private long hi8;

		private long lo8;

		public Unsigned16()
		{
			hi8 = 0;
			lo8 = 0;
		}

		public Unsigned16(long l)
		{
			hi8 = 0;
			lo8 = l;
		}

		public Unsigned16(Org.Apache.Hadoop.Examples.Terasort.Unsigned16 other)
		{
			hi8 = other.hi8;
			lo8 = other.lo8;
		}

		public override bool Equals(object o)
		{
			if (o is Org.Apache.Hadoop.Examples.Terasort.Unsigned16)
			{
				Org.Apache.Hadoop.Examples.Terasort.Unsigned16 other = (Org.Apache.Hadoop.Examples.Terasort.Unsigned16
					)o;
				return other.hi8 == hi8 && other.lo8 == lo8;
			}
			return false;
		}

		public override int GetHashCode()
		{
			return (int)lo8;
		}

		/// <summary>Parse a hex string</summary>
		/// <param name="s">the hex string</param>
		/// <exception cref="System.FormatException"/>
		public Unsigned16(string s)
		{
			Set(s);
		}

		/// <summary>Set the number from a hex string</summary>
		/// <param name="s">the number in hexadecimal</param>
		/// <exception cref="System.FormatException">if the number is invalid</exception>
		public virtual void Set(string s)
		{
			hi8 = 0;
			lo8 = 0;
			long lastDigit = unchecked((long)(0xfl)) << 60;
			for (int i = 0; i < s.Length; ++i)
			{
				int digit = GetHexDigit(s[i]);
				if ((lastDigit & hi8) != 0)
				{
					throw new FormatException(s + " overflowed 16 bytes");
				}
				hi8 <<= 4;
				hi8 |= (long)(((ulong)(lo8 & lastDigit)) >> 60);
				lo8 <<= 4;
				lo8 |= digit;
			}
		}

		/// <summary>Set the number to a given long.</summary>
		/// <param name="l">the new value, which is treated as an unsigned number</param>
		public virtual void Set(long l)
		{
			lo8 = l;
			hi8 = 0;
		}

		/// <summary>Map a hexadecimal character into a digit.</summary>
		/// <param name="ch">the character</param>
		/// <returns>the digit from 0 to 15</returns>
		/// <exception cref="System.FormatException"/>
		private static int GetHexDigit(char ch)
		{
			if (ch >= '0' && ch <= '9')
			{
				return ch - '0';
			}
			if (ch >= 'a' && ch <= 'f')
			{
				return ch - 'a' + 10;
			}
			if (ch >= 'A' && ch <= 'F')
			{
				return ch - 'A' + 10;
			}
			throw new FormatException(ch + " is not a valid hex digit");
		}

		private static readonly Org.Apache.Hadoop.Examples.Terasort.Unsigned16 Ten = new 
			Org.Apache.Hadoop.Examples.Terasort.Unsigned16(10);

		/// <exception cref="System.FormatException"/>
		public static Org.Apache.Hadoop.Examples.Terasort.Unsigned16 FromDecimal(string s
			)
		{
			Org.Apache.Hadoop.Examples.Terasort.Unsigned16 result = new Org.Apache.Hadoop.Examples.Terasort.Unsigned16
				();
			Org.Apache.Hadoop.Examples.Terasort.Unsigned16 tmp = new Org.Apache.Hadoop.Examples.Terasort.Unsigned16
				();
			for (int i = 0; i < s.Length; i++)
			{
				char ch = s[i];
				if (ch < '0' || ch > '9')
				{
					throw new FormatException(ch + " not a valid decimal digit");
				}
				int digit = ch - '0';
				result.Multiply(Ten);
				tmp.Set(digit);
				result.Add(tmp);
			}
			return result;
		}

		/// <summary>Return the number as a hex string.</summary>
		public override string ToString()
		{
			if (hi8 == 0)
			{
				return long.ToHexString(lo8);
			}
			else
			{
				StringBuilder result = new StringBuilder();
				result.Append(long.ToHexString(hi8));
				string loString = long.ToHexString(lo8);
				for (int i = loString.Length; i < 16; ++i)
				{
					result.Append('0');
				}
				result.Append(loString);
				return result.ToString();
			}
		}

		/// <summary>Get a given byte from the number.</summary>
		/// <param name="b">the byte to get with 0 meaning the most significant byte</param>
		/// <returns>the byte or 0 if b is outside of 0..15</returns>
		public virtual byte GetByte(int b)
		{
			if (b >= 0 && b < 16)
			{
				if (b < 8)
				{
					return unchecked((byte)(hi8 >> (56 - 8 * b)));
				}
				else
				{
					return unchecked((byte)(lo8 >> (120 - 8 * b)));
				}
			}
			return 0;
		}

		/// <summary>Get the hexadecimal digit at the given position.</summary>
		/// <param name="p">the digit position to get with 0 meaning the most significant</param>
		/// <returns>the character or '0' if p is outside of 0..31</returns>
		public virtual char GetHexDigit(int p)
		{
			byte digit = GetByte(p / 2);
			if (p % 2 == 0)
			{
				digit = (byte)(((ubyte)digit) >> 4);
			}
			digit &= unchecked((int)(0xf));
			if (((sbyte)digit) < 10)
			{
				return (char)('0' + digit);
			}
			else
			{
				return (char)('A' + digit - 10);
			}
		}

		/// <summary>Get the high 8 bytes as a long.</summary>
		public virtual long GetHigh8()
		{
			return hi8;
		}

		/// <summary>Get the low 8 bytes as a long.</summary>
		public virtual long GetLow8()
		{
			return lo8;
		}

		/// <summary>Multiple the current number by a 16 byte unsigned integer.</summary>
		/// <remarks>
		/// Multiple the current number by a 16 byte unsigned integer. Overflow is not
		/// detected and the result is the low 16 bytes of the result. The numbers
		/// are divided into 32 and 31 bit chunks so that the product of two chucks
		/// fits in the unsigned 63 bits of a long.
		/// </remarks>
		/// <param name="b">the other number</param>
		internal virtual void Multiply(Org.Apache.Hadoop.Examples.Terasort.Unsigned16 b)
		{
			// divide the left into 4 32 bit chunks
			long[] left = new long[4];
			left[0] = lo8 & unchecked((long)(0xffffffffl));
			left[1] = (long)(((ulong)lo8) >> 32);
			left[2] = hi8 & unchecked((long)(0xffffffffl));
			left[3] = (long)(((ulong)hi8) >> 32);
			// divide the right into 5 31 bit chunks
			long[] right = new long[5];
			right[0] = b.lo8 & unchecked((long)(0x7fffffffl));
			right[1] = ((long)(((ulong)b.lo8) >> 31)) & unchecked((long)(0x7fffffffl));
			right[2] = ((long)(((ulong)b.lo8) >> 62)) + ((b.hi8 & unchecked((long)(0x1fffffffl
				))) << 2);
			right[3] = ((long)(((ulong)b.hi8) >> 29)) & unchecked((long)(0x7fffffffl));
			right[4] = ((long)(((ulong)b.hi8) >> 60));
			// clear the cur value
			Set(0);
			Org.Apache.Hadoop.Examples.Terasort.Unsigned16 tmp = new Org.Apache.Hadoop.Examples.Terasort.Unsigned16
				();
			for (int l = 0; l < 4; ++l)
			{
				for (int r = 0; r < 5; ++r)
				{
					long prod = left[l] * right[r];
					if (prod != 0)
					{
						int off = l * 32 + r * 31;
						tmp.Set(prod);
						tmp.ShiftLeft(off);
						Add(tmp);
					}
				}
			}
		}

		/// <summary>Add the given number into the current number.</summary>
		/// <param name="b">the other number</param>
		public virtual void Add(Org.Apache.Hadoop.Examples.Terasort.Unsigned16 b)
		{
			long sumHi;
			long sumLo;
			long reshibit;
			long hibit0;
			long hibit1;
			sumHi = hi8 + b.hi8;
			hibit0 = (lo8 & unchecked((long)(0x8000000000000000L)));
			hibit1 = (b.lo8 & unchecked((long)(0x8000000000000000L)));
			sumLo = lo8 + b.lo8;
			reshibit = (sumLo & unchecked((long)(0x8000000000000000L)));
			if ((hibit0 & hibit1) != 0 | ((hibit0 ^ hibit1) != 0 && reshibit == 0))
			{
				sumHi++;
			}
			/* add carry bit */
			hi8 = sumHi;
			lo8 = sumLo;
		}

		/// <summary>Shift the number a given number of bit positions.</summary>
		/// <remarks>
		/// Shift the number a given number of bit positions. The number is the low
		/// order bits of the result.
		/// </remarks>
		/// <param name="bits">the bit positions to shift by</param>
		public virtual void ShiftLeft(int bits)
		{
			if (bits != 0)
			{
				if (bits < 64)
				{
					hi8 <<= bits;
					hi8 |= ((long)(((ulong)lo8) >> (64 - bits)));
					lo8 <<= bits;
				}
				else
				{
					if (bits < 128)
					{
						hi8 = lo8 << (bits - 64);
						lo8 = 0;
					}
					else
					{
						hi8 = 0;
						lo8 = 0;
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			hi8 = @in.ReadLong();
			lo8 = @in.ReadLong();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteLong(hi8);
			@out.WriteLong(lo8);
		}
	}
}
