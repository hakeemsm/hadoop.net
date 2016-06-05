using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Partition
{
	/// <summary>
	/// This comparator implementation provides a subset of the features provided
	/// by the Unix/GNU Sort.
	/// </summary>
	/// <remarks>
	/// This comparator implementation provides a subset of the features provided
	/// by the Unix/GNU Sort. In particular, the supported features are:
	/// -n, (Sort numerically)
	/// -r, (Reverse the result of comparison)
	/// -k pos1[,pos2], where pos is of the form f[.c][opts], where f is the number
	/// of the field to use, and c is the number of the first character from the
	/// beginning of the field. Fields and character posns are numbered starting
	/// with 1; a character position of zero in pos2 indicates the field's last
	/// character. If '.c' is omitted from pos1, it defaults to 1 (the beginning
	/// of the field); if omitted from pos2, it defaults to 0 (the end of the
	/// field). opts are ordering options (any of 'nr' as described above).
	/// We assume that the fields in the key are separated by
	/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.MapOutputKeyFieldSeperator"/>
	/// .
	/// </remarks>
	public class KeyFieldBasedComparator<K, V> : WritableComparator, Configurable
	{
		private KeyFieldHelper keyFieldHelper = new KeyFieldHelper();

		public static string ComparatorOptions = "mapreduce.partition.keycomparator.options";

		private const byte Negative = unchecked((byte)(byte)('-'));

		private const byte Zero = unchecked((byte)(byte)('0'));

		private const byte Decimal = unchecked((byte)(byte)('.'));

		private Configuration conf;

		public override void SetConf(Configuration conf)
		{
			this.conf = conf;
			string option = conf.Get(ComparatorOptions);
			string keyFieldSeparator = conf.Get(MRJobConfig.MapOutputKeyFieldSeperator, "\t");
			keyFieldHelper.SetKeyFieldSeparator(keyFieldSeparator);
			keyFieldHelper.ParseOption(option);
		}

		public override Configuration GetConf()
		{
			return conf;
		}

		public KeyFieldBasedComparator()
			: base(typeof(Text))
		{
		}

		public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			int n1 = WritableUtils.DecodeVIntSize(b1[s1]);
			int n2 = WritableUtils.DecodeVIntSize(b2[s2]);
			IList<KeyFieldHelper.KeyDescription> allKeySpecs = keyFieldHelper.KeySpecs();
			if (allKeySpecs.Count == 0)
			{
				return CompareBytes(b1, s1 + n1, l1 - n1, b2, s2 + n2, l2 - n2);
			}
			int[] lengthIndicesFirst = keyFieldHelper.GetWordLengths(b1, s1 + n1, s1 + l1);
			int[] lengthIndicesSecond = keyFieldHelper.GetWordLengths(b2, s2 + n2, s2 + l2);
			foreach (KeyFieldHelper.KeyDescription keySpec in allKeySpecs)
			{
				int startCharFirst = keyFieldHelper.GetStartOffset(b1, s1 + n1, s1 + l1, lengthIndicesFirst
					, keySpec);
				int endCharFirst = keyFieldHelper.GetEndOffset(b1, s1 + n1, s1 + l1, lengthIndicesFirst
					, keySpec);
				int startCharSecond = keyFieldHelper.GetStartOffset(b2, s2 + n2, s2 + l2, lengthIndicesSecond
					, keySpec);
				int endCharSecond = keyFieldHelper.GetEndOffset(b2, s2 + n2, s2 + l2, lengthIndicesSecond
					, keySpec);
				int result;
				if ((result = CompareByteSequence(b1, startCharFirst, endCharFirst, b2, startCharSecond
					, endCharSecond, keySpec)) != 0)
				{
					return result;
				}
			}
			return 0;
		}

		private int CompareByteSequence(byte[] first, int start1, int end1, byte[] second
			, int start2, int end2, KeyFieldHelper.KeyDescription key)
		{
			if (start1 == -1)
			{
				if (key.reverse)
				{
					return 1;
				}
				return -1;
			}
			if (start2 == -1)
			{
				if (key.reverse)
				{
					return -1;
				}
				return 1;
			}
			int compareResult = 0;
			if (!key.numeric)
			{
				compareResult = CompareBytes(first, start1, end1 - start1 + 1, second, start2, end2
					 - start2 + 1);
			}
			if (key.numeric)
			{
				compareResult = NumericalCompare(first, start1, end1, second, start2, end2);
			}
			if (key.reverse)
			{
				return -compareResult;
			}
			return compareResult;
		}

		private int NumericalCompare(byte[] a, int start1, int end1, byte[] b, int start2
			, int end2)
		{
			int i = start1;
			int j = start2;
			int mul = 1;
			byte first_a = a[i];
			byte first_b = b[j];
			if (first_a == Negative)
			{
				if (first_b != Negative)
				{
					//check for cases like -0.0 and 0.0 (they should be declared equal)
					return OneNegativeCompare(a, start1 + 1, end1, b, start2, end2);
				}
				i++;
			}
			if (first_b == Negative)
			{
				if (first_a != Negative)
				{
					//check for cases like 0.0 and -0.0 (they should be declared equal)
					return -OneNegativeCompare(b, start2 + 1, end2, a, start1, end1);
				}
				j++;
			}
			if (first_b == Negative && first_a == Negative)
			{
				mul = -1;
			}
			//skip over ZEROs
			while (i <= end1)
			{
				if (a[i] != Zero)
				{
					break;
				}
				i++;
			}
			while (j <= end2)
			{
				if (b[j] != Zero)
				{
					break;
				}
				j++;
			}
			//skip over equal characters and stopping at the first nondigit char
			//The nondigit character could be '.'
			while (i <= end1 && j <= end2)
			{
				if (!Isdigit(a[i]) || a[i] != b[j])
				{
					break;
				}
				i++;
				j++;
			}
			if (i <= end1)
			{
				first_a = a[i];
			}
			if (j <= end2)
			{
				first_b = b[j];
			}
			//store the result of the difference. This could be final result if the
			//number of digits in the mantissa is the same in both the numbers 
			int firstResult = first_a - first_b;
			//check whether we hit a decimal in the earlier scan
			if ((first_a == Decimal && (!Isdigit(first_b) || j > end2)) || (first_b == Decimal
				 && (!Isdigit(first_a) || i > end1)))
			{
				return ((mul < 0) ? -DecimalCompare(a, i, end1, b, j, end2) : DecimalCompare(a, i
					, end1, b, j, end2));
			}
			//check the number of digits in the mantissa of the numbers
			int numRemainDigits_a = 0;
			int numRemainDigits_b = 0;
			while (i <= end1)
			{
				//if we encounter a non-digit treat the corresponding number as being 
				//smaller      
				if (Isdigit(a[i++]))
				{
					numRemainDigits_a++;
				}
				else
				{
					break;
				}
			}
			while (j <= end2)
			{
				//if we encounter a non-digit treat the corresponding number as being 
				//smaller
				if (Isdigit(b[j++]))
				{
					numRemainDigits_b++;
				}
				else
				{
					break;
				}
			}
			int ret = numRemainDigits_a - numRemainDigits_b;
			if (ret == 0)
			{
				return ((mul < 0) ? -firstResult : firstResult);
			}
			else
			{
				return ((mul < 0) ? -ret : ret);
			}
		}

		private bool Isdigit(byte b)
		{
			if ('0' <= b && ((sbyte)b) <= '9')
			{
				return true;
			}
			return false;
		}

		private int DecimalCompare(byte[] a, int i, int end1, byte[] b, int j, int end2)
		{
			if (i > end1)
			{
				//if a[] has nothing remaining
				return -DecimalCompare1(b, ++j, end2);
			}
			if (j > end2)
			{
				//if b[] has nothing remaining
				return DecimalCompare1(a, ++i, end1);
			}
			if (a[i] == Decimal && b[j] == Decimal)
			{
				while (i <= end1 && j <= end2)
				{
					if (a[i] != b[j])
					{
						if (Isdigit(a[i]) && Isdigit(b[j]))
						{
							return a[i] - b[j];
						}
						if (Isdigit(a[i]))
						{
							return 1;
						}
						if (Isdigit(b[j]))
						{
							return -1;
						}
						return 0;
					}
					i++;
					j++;
				}
				if (i > end1 && j > end2)
				{
					return 0;
				}
				if (i > end1)
				{
					//check whether there is a non-ZERO digit after potentially
					//a number of ZEROs (e.g., a=.4444, b=.444400004)
					return -DecimalCompare1(b, j, end2);
				}
				if (j > end2)
				{
					//check whether there is a non-ZERO digit after potentially
					//a number of ZEROs (e.g., b=.4444, a=.444400004)
					return DecimalCompare1(a, i, end1);
				}
			}
			else
			{
				if (a[i] == Decimal)
				{
					return DecimalCompare1(a, ++i, end1);
				}
				else
				{
					if (b[j] == Decimal)
					{
						return -DecimalCompare1(b, ++j, end2);
					}
				}
			}
			return 0;
		}

		private int DecimalCompare1(byte[] a, int i, int end)
		{
			while (i <= end)
			{
				if (a[i] == Zero)
				{
					i++;
					continue;
				}
				if (Isdigit(a[i]))
				{
					return 1;
				}
				else
				{
					return 0;
				}
			}
			return 0;
		}

		private int OneNegativeCompare(byte[] a, int start1, int end1, byte[] b, int start2
			, int end2)
		{
			//here a[] is negative and b[] is positive
			//We have to ascertain whether the number contains any digits.
			//If it does, then it is a smaller number for sure. If not,
			//then we need to scan b[] to find out whether b[] has a digit
			//If b[] does contain a digit, then b[] is certainly
			//greater. If not, that is, both a[] and b[] don't contain
			//digits then they should be considered equal.
			if (!IsZero(a, start1, end1))
			{
				return -1;
			}
			//reached here - this means that a[] is a ZERO
			if (!IsZero(b, start2, end2))
			{
				return -1;
			}
			//reached here - both numbers are basically ZEROs and hence
			//they should compare equal
			return 0;
		}

		private bool IsZero(byte[] a, int start, int end)
		{
			//check for zeros in the significand part as well as the decimal part
			//note that we treat the non-digit characters as ZERO
			int i = start;
			//we check the significand for being a ZERO
			while (i <= end)
			{
				if (a[i] != Zero)
				{
					if (a[i] != Decimal && Isdigit(a[i]))
					{
						return false;
					}
					break;
				}
				i++;
			}
			if (i != (end + 1) && a[i++] == Decimal)
			{
				//we check the decimal part for being a ZERO
				while (i <= end)
				{
					if (a[i] != Zero)
					{
						if (Isdigit(a[i]))
						{
							return false;
						}
						break;
					}
					i++;
				}
			}
			return true;
		}

		/// <summary>
		/// Set the
		/// <see cref="KeyFieldBasedComparator{K, V}"/>
		/// options used to compare keys.
		/// </summary>
		/// <param name="keySpec">
		/// the key specification of the form -k pos1[,pos2], where,
		/// pos is of the form f[.c][opts], where f is the number
		/// of the key field to use, and c is the number of the first character from
		/// the beginning of the field. Fields and character posns are numbered
		/// starting with 1; a character position of zero in pos2 indicates the
		/// field's last character. If '.c' is omitted from pos1, it defaults to 1
		/// (the beginning of the field); if omitted from pos2, it defaults to 0
		/// (the end of the field). opts are ordering options. The supported options
		/// are:
		/// -n, (Sort numerically)
		/// -r, (Reverse the result of comparison)
		/// </param>
		public static void SetKeyFieldComparatorOptions(Job job, string keySpec)
		{
			job.GetConfiguration().Set(ComparatorOptions, keySpec);
		}

		/// <summary>
		/// Get the
		/// <see cref="KeyFieldBasedComparator{K, V}"/>
		/// options
		/// </summary>
		public static string GetKeyFieldComparatorOption(JobContext job)
		{
			return job.GetConfiguration().Get(ComparatorOptions);
		}
	}
}
