using System.IO;
using Mono.Math;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.Terasort
{
	/// <summary>A single process data generator for the terasort data.</summary>
	/// <remarks>
	/// A single process data generator for the terasort data. Based on gensort.c
	/// version 1.1 (3 Mar 2009) from Chris Nyberg &lt;chris.nyberg@ordinal.com&gt;.
	/// </remarks>
	public class GenSort
	{
		/// <summary>
		/// Generate a "binary" record suitable for all sort benchmarks *except*
		/// PennySort.
		/// </summary>
		internal static void GenerateRecord(byte[] recBuf, Unsigned16 rand, Unsigned16 recordNumber
			)
		{
			/* generate the 10-byte key using the high 10 bytes of the 128-bit
			* random number
			*/
			for (int i = 0; i < 10; ++i)
			{
				recBuf[i] = rand.GetByte(i);
			}
			/* add 2 bytes of "break" */
			recBuf[10] = unchecked((int)(0x00));
			recBuf[11] = unchecked((int)(0x11));
			/* convert the 128-bit record number to 32 bits of ascii hexadecimal
			* as the next 32 bytes of the record.
			*/
			for (int i_1 = 0; i_1 < 32; i_1++)
			{
				recBuf[12 + i_1] = unchecked((byte)recordNumber.GetHexDigit(i_1));
			}
			/* add 4 bytes of "break" data */
			recBuf[44] = unchecked((byte)unchecked((int)(0x88)));
			recBuf[45] = unchecked((byte)unchecked((int)(0x99)));
			recBuf[46] = unchecked((byte)unchecked((int)(0xAA)));
			recBuf[47] = unchecked((byte)unchecked((int)(0xBB)));
			/* add 48 bytes of filler based on low 48 bits of random number */
			for (int i_2 = 0; i_2 < 12; ++i_2)
			{
				recBuf[48 + i_2 * 4] = recBuf[49 + i_2 * 4] = recBuf[50 + i_2 * 4] = recBuf[51 + 
					i_2 * 4] = unchecked((byte)rand.GetHexDigit(20 + i_2));
			}
			/* add 4 bytes of "break" data */
			recBuf[96] = unchecked((byte)unchecked((int)(0xCC)));
			recBuf[97] = unchecked((byte)unchecked((int)(0xDD)));
			recBuf[98] = unchecked((byte)unchecked((int)(0xEE)));
			recBuf[99] = unchecked((byte)unchecked((int)(0xFF)));
		}

		private static BigInteger MakeBigInteger(long x)
		{
			byte[] data = new byte[8];
			for (int i = 0; i < 8; ++i)
			{
				data[i] = unchecked((byte)((long)(((ulong)x) >> (56 - 8 * i))));
			}
			return new BigInteger(1, data);
		}

		private static readonly BigInteger NinetyFive = new BigInteger("95");

		/// <summary>
		/// Generate an ascii record suitable for all sort benchmarks including
		/// PennySort.
		/// </summary>
		internal static void GenerateAsciiRecord(byte[] recBuf, Unsigned16 rand, Unsigned16
			 recordNumber)
		{
			/* generate the 10-byte ascii key using mostly the high 64 bits.
			*/
			long temp = rand.GetHigh8();
			if (temp < 0)
			{
				// use biginteger to avoid the negative sign problem
				BigInteger bigTemp = MakeBigInteger(temp);
				recBuf[0] = unchecked((byte)((byte)(' ') + (bigTemp.Mod(NinetyFive))));
				temp = bigTemp.Divide(NinetyFive);
			}
			else
			{
				recBuf[0] = unchecked((byte)((byte)(' ') + (temp % 95)));
				temp /= 95;
			}
			for (int i = 1; i < 8; ++i)
			{
				recBuf[i] = unchecked((byte)((byte)(' ') + (temp % 95)));
				temp /= 95;
			}
			temp = rand.GetLow8();
			if (temp < 0)
			{
				BigInteger bigTemp = MakeBigInteger(temp);
				recBuf[8] = unchecked((byte)((byte)(' ') + (bigTemp.Mod(NinetyFive))));
				temp = bigTemp.Divide(NinetyFive);
			}
			else
			{
				recBuf[8] = unchecked((byte)((byte)(' ') + (temp % 95)));
				temp /= 95;
			}
			recBuf[9] = unchecked((byte)((byte)(' ') + (temp % 95)));
			/* add 2 bytes of "break" */
			recBuf[10] = (byte)(' ');
			recBuf[11] = (byte)(' ');
			/* convert the 128-bit record number to 32 bits of ascii hexadecimal
			* as the next 32 bytes of the record.
			*/
			for (int i_1 = 0; i_1 < 32; i_1++)
			{
				recBuf[12 + i_1] = unchecked((byte)recordNumber.GetHexDigit(i_1));
			}
			/* add 2 bytes of "break" data */
			recBuf[44] = (byte)(' ');
			recBuf[45] = (byte)(' ');
			/* add 52 bytes of filler based on low 48 bits of random number */
			for (int i_2 = 0; i_2 < 13; ++i_2)
			{
				recBuf[46 + i_2 * 4] = recBuf[47 + i_2 * 4] = recBuf[48 + i_2 * 4] = recBuf[49 + 
					i_2 * 4] = unchecked((byte)rand.GetHexDigit(19 + i_2));
			}
			/* add 2 bytes of "break" data */
			recBuf[98] = (byte)('\r');
			/* nice for Windows */
			recBuf[99] = (byte)('\n');
		}

		private static void Usage()
		{
			TextWriter @out = System.Console.Out;
			@out.WriteLine("usage: gensort [-a] [-c] [-bSTARTING_REC_NUM] NUM_RECS FILE_NAME"
				);
			@out.WriteLine("-a        Generate ascii records required for PennySort or JouleSort."
				);
			@out.WriteLine("          These records are also an alternative input for the other"
				);
			@out.WriteLine("          sort benchmarks.  Without this flag, binary records will be"
				);
			@out.WriteLine("          generated that contain the highest density of randomness in"
				);
			@out.WriteLine("          the 10-byte key.");
			@out.WriteLine("-c        Calculate the sum of the crc32 checksums of each of the"
				);
			@out.WriteLine("          generated records and send it to standard error.");
			@out.WriteLine("-bN       Set the beginning record generated to N. By default the"
				);
			@out.WriteLine("          first record generated is record 0.");
			@out.WriteLine("NUM_RECS  The number of sequential records to generate.");
			@out.WriteLine("FILE_NAME The name of the file to write the records to.\n");
			@out.WriteLine("Example 1 - to generate 1000000 ascii records starting at record 0 to"
				);
			@out.WriteLine("the file named \"pennyinput\":");
			@out.WriteLine("    gensort -a 1000000 pennyinput\n");
			@out.WriteLine("Example 2 - to generate 1000 binary records beginning with record 2000"
				);
			@out.WriteLine("to the file named \"partition2\":");
			@out.WriteLine("    gensort -b2000 1000 partition2");
			System.Environment.Exit(1);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void OutputRecords(OutputStream @out, bool useAscii, Unsigned16 firstRecordNumber
			, Unsigned16 recordsToGenerate, Unsigned16 checksum)
		{
			byte[] row = new byte[100];
			Unsigned16 recordNumber = new Unsigned16(firstRecordNumber);
			Unsigned16 lastRecordNumber = new Unsigned16(firstRecordNumber);
			Checksum crc = new PureJavaCrc32();
			Unsigned16 tmp = new Unsigned16();
			lastRecordNumber.Add(recordsToGenerate);
			Unsigned16 One = new Unsigned16(1);
			Unsigned16 rand = Random16.SkipAhead(firstRecordNumber);
			while (!recordNumber.Equals(lastRecordNumber))
			{
				Random16.NextRand(rand);
				if (useAscii)
				{
					GenerateAsciiRecord(row, rand, recordNumber);
				}
				else
				{
					GenerateRecord(row, rand, recordNumber);
				}
				if (checksum != null)
				{
					crc.Reset();
					crc.Update(row, 0, row.Length);
					tmp.Set(crc.GetValue());
					checksum.Add(tmp);
				}
				recordNumber.Add(One);
				@out.Write(row);
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			Unsigned16 startingRecord = new Unsigned16();
			Unsigned16 numberOfRecords;
			OutputStream @out;
			bool useAscii = false;
			Unsigned16 checksum = null;
			int i;
			for (i = 0; i < args.Length; ++i)
			{
				string arg = args[i];
				int argLength = arg.Length;
				if (argLength >= 1 && arg[0] == '-')
				{
					if (argLength < 2)
					{
						Usage();
					}
					switch (arg[1])
					{
						case 'a':
						{
							useAscii = true;
							break;
						}

						case 'b':
						{
							startingRecord = Unsigned16.FromDecimal(Sharpen.Runtime.Substring(arg, 2));
							break;
						}

						case 'c':
						{
							checksum = new Unsigned16();
							break;
						}

						default:
						{
							Usage();
							break;
						}
					}
				}
				else
				{
					break;
				}
			}
			if (args.Length - i != 2)
			{
				Usage();
			}
			numberOfRecords = Unsigned16.FromDecimal(args[i]);
			@out = new FileOutputStream(args[i + 1]);
			OutputRecords(@out, useAscii, startingRecord, numberOfRecords, checksum);
			@out.Close();
			if (checksum != null)
			{
				System.Console.Out.WriteLine(checksum);
			}
		}
	}
}
