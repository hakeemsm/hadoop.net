using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// A pure-java implementation of the CRC32 checksum that uses
	/// the same polynomial as the built-in native CRC32.
	/// </summary>
	/// <remarks>
	/// A pure-java implementation of the CRC32 checksum that uses
	/// the same polynomial as the built-in native CRC32.
	/// This is to avoid the JNI overhead for certain uses of Checksumming
	/// where many small pieces of data are checksummed in succession.
	/// The current version is ~10x to 1.8x as fast as Sun's native
	/// java.util.zip.CRC32 in Java 1.6
	/// </remarks>
	/// <seealso cref="Sharpen.CRC32"/>
	public class PureJavaCrc32 : Checksum
	{
		/// <summary>the current CRC value, bit-flipped</summary>
		private int crc;

		/// <summary>Create a new PureJavaCrc32 object.</summary>
		public PureJavaCrc32()
		{
			Reset();
		}

		public virtual long GetValue()
		{
			return (~crc) & unchecked((long)(0xffffffffL));
		}

		public virtual void Reset()
		{
			crc = unchecked((int)(0xffffffff));
		}

		public virtual void Update(byte[] b, int offset, int len)
		{
			int localCrc = crc;
			int remainder = len & unchecked((int)(0x7));
			int i = offset;
			for (int end = offset + len - remainder; i < end; i += 8)
			{
				int x = localCrc ^ ((((int)(((uint)(b[i] << 24)) >> 24)) + ((int)(((uint)(b[i + 1
					] << 24)) >> 16))) + (((int)(((uint)(b[i + 2] << 24)) >> 8)) + (b[i + 3] << 24))
					);
				localCrc = ((T[((int)(((uint)(x << 24)) >> 24)) + unchecked((int)(0x700))] ^ T[((
					int)(((uint)(x << 16)) >> 24)) + unchecked((int)(0x600))]) ^ (T[((int)(((uint)(x
					 << 8)) >> 24)) + unchecked((int)(0x500))] ^ T[((int)(((uint)x) >> 24)) + unchecked(
					(int)(0x400))])) ^ ((T[((int)(((uint)(b[i + 4] << 24)) >> 24)) + unchecked((int)
					(0x300))] ^ T[((int)(((uint)(b[i + 5] << 24)) >> 24)) + unchecked((int)(0x200))]
					) ^ (T[((int)(((uint)(b[i + 6] << 24)) >> 24)) + unchecked((int)(0x100))] ^ T[((
					int)(((uint)(b[i + 7] << 24)) >> 24))]));
			}
			switch (remainder)
			{
				case 7:
				{
					/* loop unroll - duff's device style */
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[(int)(((uint)((localCrc ^ b[i++]) <<
						 24)) >> 24)];
					goto case 6;
				}

				case 6:
				{
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[(int)(((uint)((localCrc ^ b[i++]) <<
						 24)) >> 24)];
					goto case 5;
				}

				case 5:
				{
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[(int)(((uint)((localCrc ^ b[i++]) <<
						 24)) >> 24)];
					goto case 4;
				}

				case 4:
				{
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[(int)(((uint)((localCrc ^ b[i++]) <<
						 24)) >> 24)];
					goto case 3;
				}

				case 3:
				{
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[(int)(((uint)((localCrc ^ b[i++]) <<
						 24)) >> 24)];
					goto case 2;
				}

				case 2:
				{
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[(int)(((uint)((localCrc ^ b[i++]) <<
						 24)) >> 24)];
					goto case 1;
				}

				case 1:
				{
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[(int)(((uint)((localCrc ^ b[i++]) <<
						 24)) >> 24)];
					goto default;
				}

				default:
				{
					break;
				}
			}
			/* nothing */
			// Publish crc out to object
			crc = localCrc;
		}

		public void Update(int b)
		{
			crc = ((int)(((uint)crc) >> 8)) ^ T[((int)(((uint)((crc ^ b) << 24)) >> 24))];
		}

		private static readonly int[] T = new int[] { unchecked((int)(0x00000000)), unchecked(
			(int)(0x77073096)), unchecked((int)(0xEE0E612C)), unchecked((int)(0x990951BA)), 
			unchecked((int)(0x076DC419)), unchecked((int)(0x706AF48F)), unchecked((int)(0xE963A535
			)), unchecked((int)(0x9E6495A3)), unchecked((int)(0x0EDB8832)), unchecked((int)(
			0x79DCB8A4)), unchecked((int)(0xE0D5E91E)), unchecked((int)(0x97D2D988)), unchecked(
			(int)(0x09B64C2B)), unchecked((int)(0x7EB17CBD)), unchecked((int)(0xE7B82D07)), 
			unchecked((int)(0x90BF1D91)), unchecked((int)(0x1DB71064)), unchecked((int)(0x6AB020F2
			)), unchecked((int)(0xF3B97148)), unchecked((int)(0x84BE41DE)), unchecked((int)(
			0x1ADAD47D)), unchecked((int)(0x6DDDE4EB)), unchecked((int)(0xF4D4B551)), unchecked(
			(int)(0x83D385C7)), unchecked((int)(0x136C9856)), unchecked((int)(0x646BA8C0)), 
			unchecked((int)(0xFD62F97A)), unchecked((int)(0x8A65C9EC)), unchecked((int)(0x14015C4F
			)), unchecked((int)(0x63066CD9)), unchecked((int)(0xFA0F3D63)), unchecked((int)(
			0x8D080DF5)), unchecked((int)(0x3B6E20C8)), unchecked((int)(0x4C69105E)), unchecked(
			(int)(0xD56041E4)), unchecked((int)(0xA2677172)), unchecked((int)(0x3C03E4D1)), 
			unchecked((int)(0x4B04D447)), unchecked((int)(0xD20D85FD)), unchecked((int)(0xA50AB56B
			)), unchecked((int)(0x35B5A8FA)), unchecked((int)(0x42B2986C)), unchecked((int)(
			0xDBBBC9D6)), unchecked((int)(0xACBCF940)), unchecked((int)(0x32D86CE3)), unchecked(
			(int)(0x45DF5C75)), unchecked((int)(0xDCD60DCF)), unchecked((int)(0xABD13D59)), 
			unchecked((int)(0x26D930AC)), unchecked((int)(0x51DE003A)), unchecked((int)(0xC8D75180
			)), unchecked((int)(0xBFD06116)), unchecked((int)(0x21B4F4B5)), unchecked((int)(
			0x56B3C423)), unchecked((int)(0xCFBA9599)), unchecked((int)(0xB8BDA50F)), unchecked(
			(int)(0x2802B89E)), unchecked((int)(0x5F058808)), unchecked((int)(0xC60CD9B2)), 
			unchecked((int)(0xB10BE924)), unchecked((int)(0x2F6F7C87)), unchecked((int)(0x58684C11
			)), unchecked((int)(0xC1611DAB)), unchecked((int)(0xB6662D3D)), unchecked((int)(
			0x76DC4190)), unchecked((int)(0x01DB7106)), unchecked((int)(0x98D220BC)), unchecked(
			(int)(0xEFD5102A)), unchecked((int)(0x71B18589)), unchecked((int)(0x06B6B51F)), 
			unchecked((int)(0x9FBFE4A5)), unchecked((int)(0xE8B8D433)), unchecked((int)(0x7807C9A2
			)), unchecked((int)(0x0F00F934)), unchecked((int)(0x9609A88E)), unchecked((int)(
			0xE10E9818)), unchecked((int)(0x7F6A0DBB)), unchecked((int)(0x086D3D2D)), unchecked(
			(int)(0x91646C97)), unchecked((int)(0xE6635C01)), unchecked((int)(0x6B6B51F4)), 
			unchecked((int)(0x1C6C6162)), unchecked((int)(0x856530D8)), unchecked((int)(0xF262004E
			)), unchecked((int)(0x6C0695ED)), unchecked((int)(0x1B01A57B)), unchecked((int)(
			0x8208F4C1)), unchecked((int)(0xF50FC457)), unchecked((int)(0x65B0D9C6)), unchecked(
			(int)(0x12B7E950)), unchecked((int)(0x8BBEB8EA)), unchecked((int)(0xFCB9887C)), 
			unchecked((int)(0x62DD1DDF)), unchecked((int)(0x15DA2D49)), unchecked((int)(0x8CD37CF3
			)), unchecked((int)(0xFBD44C65)), unchecked((int)(0x4DB26158)), unchecked((int)(
			0x3AB551CE)), unchecked((int)(0xA3BC0074)), unchecked((int)(0xD4BB30E2)), unchecked(
			(int)(0x4ADFA541)), unchecked((int)(0x3DD895D7)), unchecked((int)(0xA4D1C46D)), 
			unchecked((int)(0xD3D6F4FB)), unchecked((int)(0x4369E96A)), unchecked((int)(0x346ED9FC
			)), unchecked((int)(0xAD678846)), unchecked((int)(0xDA60B8D0)), unchecked((int)(
			0x44042D73)), unchecked((int)(0x33031DE5)), unchecked((int)(0xAA0A4C5F)), unchecked(
			(int)(0xDD0D7CC9)), unchecked((int)(0x5005713C)), unchecked((int)(0x270241AA)), 
			unchecked((int)(0xBE0B1010)), unchecked((int)(0xC90C2086)), unchecked((int)(0x5768B525
			)), unchecked((int)(0x206F85B3)), unchecked((int)(0xB966D409)), unchecked((int)(
			0xCE61E49F)), unchecked((int)(0x5EDEF90E)), unchecked((int)(0x29D9C998)), unchecked(
			(int)(0xB0D09822)), unchecked((int)(0xC7D7A8B4)), unchecked((int)(0x59B33D17)), 
			unchecked((int)(0x2EB40D81)), unchecked((int)(0xB7BD5C3B)), unchecked((int)(0xC0BA6CAD
			)), unchecked((int)(0xEDB88320)), unchecked((int)(0x9ABFB3B6)), unchecked((int)(
			0x03B6E20C)), unchecked((int)(0x74B1D29A)), unchecked((int)(0xEAD54739)), unchecked(
			(int)(0x9DD277AF)), unchecked((int)(0x04DB2615)), unchecked((int)(0x73DC1683)), 
			unchecked((int)(0xE3630B12)), unchecked((int)(0x94643B84)), unchecked((int)(0x0D6D6A3E
			)), unchecked((int)(0x7A6A5AA8)), unchecked((int)(0xE40ECF0B)), unchecked((int)(
			0x9309FF9D)), unchecked((int)(0x0A00AE27)), unchecked((int)(0x7D079EB1)), unchecked(
			(int)(0xF00F9344)), unchecked((int)(0x8708A3D2)), unchecked((int)(0x1E01F268)), 
			unchecked((int)(0x6906C2FE)), unchecked((int)(0xF762575D)), unchecked((int)(0x806567CB
			)), unchecked((int)(0x196C3671)), unchecked((int)(0x6E6B06E7)), unchecked((int)(
			0xFED41B76)), unchecked((int)(0x89D32BE0)), unchecked((int)(0x10DA7A5A)), unchecked(
			(int)(0x67DD4ACC)), unchecked((int)(0xF9B9DF6F)), unchecked((int)(0x8EBEEFF9)), 
			unchecked((int)(0x17B7BE43)), unchecked((int)(0x60B08ED5)), unchecked((int)(0xD6D6A3E8
			)), unchecked((int)(0xA1D1937E)), unchecked((int)(0x38D8C2C4)), unchecked((int)(
			0x4FDFF252)), unchecked((int)(0xD1BB67F1)), unchecked((int)(0xA6BC5767)), unchecked(
			(int)(0x3FB506DD)), unchecked((int)(0x48B2364B)), unchecked((int)(0xD80D2BDA)), 
			unchecked((int)(0xAF0A1B4C)), unchecked((int)(0x36034AF6)), unchecked((int)(0x41047A60
			)), unchecked((int)(0xDF60EFC3)), unchecked((int)(0xA867DF55)), unchecked((int)(
			0x316E8EEF)), unchecked((int)(0x4669BE79)), unchecked((int)(0xCB61B38C)), unchecked(
			(int)(0xBC66831A)), unchecked((int)(0x256FD2A0)), unchecked((int)(0x5268E236)), 
			unchecked((int)(0xCC0C7795)), unchecked((int)(0xBB0B4703)), unchecked((int)(0x220216B9
			)), unchecked((int)(0x5505262F)), unchecked((int)(0xC5BA3BBE)), unchecked((int)(
			0xB2BD0B28)), unchecked((int)(0x2BB45A92)), unchecked((int)(0x5CB36A04)), unchecked(
			(int)(0xC2D7FFA7)), unchecked((int)(0xB5D0CF31)), unchecked((int)(0x2CD99E8B)), 
			unchecked((int)(0x5BDEAE1D)), unchecked((int)(0x9B64C2B0)), unchecked((int)(0xEC63F226
			)), unchecked((int)(0x756AA39C)), unchecked((int)(0x026D930A)), unchecked((int)(
			0x9C0906A9)), unchecked((int)(0xEB0E363F)), unchecked((int)(0x72076785)), unchecked(
			(int)(0x05005713)), unchecked((int)(0x95BF4A82)), unchecked((int)(0xE2B87A14)), 
			unchecked((int)(0x7BB12BAE)), unchecked((int)(0x0CB61B38)), unchecked((int)(0x92D28E9B
			)), unchecked((int)(0xE5D5BE0D)), unchecked((int)(0x7CDCEFB7)), unchecked((int)(
			0x0BDBDF21)), unchecked((int)(0x86D3D2D4)), unchecked((int)(0xF1D4E242)), unchecked(
			(int)(0x68DDB3F8)), unchecked((int)(0x1FDA836E)), unchecked((int)(0x81BE16CD)), 
			unchecked((int)(0xF6B9265B)), unchecked((int)(0x6FB077E1)), unchecked((int)(0x18B74777
			)), unchecked((int)(0x88085AE6)), unchecked((int)(0xFF0F6A70)), unchecked((int)(
			0x66063BCA)), unchecked((int)(0x11010B5C)), unchecked((int)(0x8F659EFF)), unchecked(
			(int)(0xF862AE69)), unchecked((int)(0x616BFFD3)), unchecked((int)(0x166CCF45)), 
			unchecked((int)(0xA00AE278)), unchecked((int)(0xD70DD2EE)), unchecked((int)(0x4E048354
			)), unchecked((int)(0x3903B3C2)), unchecked((int)(0xA7672661)), unchecked((int)(
			0xD06016F7)), unchecked((int)(0x4969474D)), unchecked((int)(0x3E6E77DB)), unchecked(
			(int)(0xAED16A4A)), unchecked((int)(0xD9D65ADC)), unchecked((int)(0x40DF0B66)), 
			unchecked((int)(0x37D83BF0)), unchecked((int)(0xA9BCAE53)), unchecked((int)(0xDEBB9EC5
			)), unchecked((int)(0x47B2CF7F)), unchecked((int)(0x30B5FFE9)), unchecked((int)(
			0xBDBDF21C)), unchecked((int)(0xCABAC28A)), unchecked((int)(0x53B39330)), unchecked(
			(int)(0x24B4A3A6)), unchecked((int)(0xBAD03605)), unchecked((int)(0xCDD70693)), 
			unchecked((int)(0x54DE5729)), unchecked((int)(0x23D967BF)), unchecked((int)(0xB3667A2E
			)), unchecked((int)(0xC4614AB8)), unchecked((int)(0x5D681B02)), unchecked((int)(
			0x2A6F2B94)), unchecked((int)(0xB40BBE37)), unchecked((int)(0xC30C8EA1)), unchecked(
			(int)(0x5A05DF1B)), unchecked((int)(0x2D02EF8D)), unchecked((int)(0x00000000)), 
			unchecked((int)(0x191B3141)), unchecked((int)(0x32366282)), unchecked((int)(0x2B2D53C3
			)), unchecked((int)(0x646CC504)), unchecked((int)(0x7D77F445)), unchecked((int)(
			0x565AA786)), unchecked((int)(0x4F4196C7)), unchecked((int)(0xC8D98A08)), unchecked(
			(int)(0xD1C2BB49)), unchecked((int)(0xFAEFE88A)), unchecked((int)(0xE3F4D9CB)), 
			unchecked((int)(0xACB54F0C)), unchecked((int)(0xB5AE7E4D)), unchecked((int)(0x9E832D8E
			)), unchecked((int)(0x87981CCF)), unchecked((int)(0x4AC21251)), unchecked((int)(
			0x53D92310)), unchecked((int)(0x78F470D3)), unchecked((int)(0x61EF4192)), unchecked(
			(int)(0x2EAED755)), unchecked((int)(0x37B5E614)), unchecked((int)(0x1C98B5D7)), 
			unchecked((int)(0x05838496)), unchecked((int)(0x821B9859)), unchecked((int)(0x9B00A918
			)), unchecked((int)(0xB02DFADB)), unchecked((int)(0xA936CB9A)), unchecked((int)(
			0xE6775D5D)), unchecked((int)(0xFF6C6C1C)), unchecked((int)(0xD4413FDF)), unchecked(
			(int)(0xCD5A0E9E)), unchecked((int)(0x958424A2)), unchecked((int)(0x8C9F15E3)), 
			unchecked((int)(0xA7B24620)), unchecked((int)(0xBEA97761)), unchecked((int)(0xF1E8E1A6
			)), unchecked((int)(0xE8F3D0E7)), unchecked((int)(0xC3DE8324)), unchecked((int)(
			0xDAC5B265)), unchecked((int)(0x5D5DAEAA)), unchecked((int)(0x44469FEB)), unchecked(
			(int)(0x6F6BCC28)), unchecked((int)(0x7670FD69)), unchecked((int)(0x39316BAE)), 
			unchecked((int)(0x202A5AEF)), unchecked((int)(0x0B07092C)), unchecked((int)(0x121C386D
			)), unchecked((int)(0xDF4636F3)), unchecked((int)(0xC65D07B2)), unchecked((int)(
			0xED705471)), unchecked((int)(0xF46B6530)), unchecked((int)(0xBB2AF3F7)), unchecked(
			(int)(0xA231C2B6)), unchecked((int)(0x891C9175)), unchecked((int)(0x9007A034)), 
			unchecked((int)(0x179FBCFB)), unchecked((int)(0x0E848DBA)), unchecked((int)(0x25A9DE79
			)), unchecked((int)(0x3CB2EF38)), unchecked((int)(0x73F379FF)), unchecked((int)(
			0x6AE848BE)), unchecked((int)(0x41C51B7D)), unchecked((int)(0x58DE2A3C)), unchecked(
			(int)(0xF0794F05)), unchecked((int)(0xE9627E44)), unchecked((int)(0xC24F2D87)), 
			unchecked((int)(0xDB541CC6)), unchecked((int)(0x94158A01)), unchecked((int)(0x8D0EBB40
			)), unchecked((int)(0xA623E883)), unchecked((int)(0xBF38D9C2)), unchecked((int)(
			0x38A0C50D)), unchecked((int)(0x21BBF44C)), unchecked((int)(0x0A96A78F)), unchecked(
			(int)(0x138D96CE)), unchecked((int)(0x5CCC0009)), unchecked((int)(0x45D73148)), 
			unchecked((int)(0x6EFA628B)), unchecked((int)(0x77E153CA)), unchecked((int)(0xBABB5D54
			)), unchecked((int)(0xA3A06C15)), unchecked((int)(0x888D3FD6)), unchecked((int)(
			0x91960E97)), unchecked((int)(0xDED79850)), unchecked((int)(0xC7CCA911)), unchecked(
			(int)(0xECE1FAD2)), unchecked((int)(0xF5FACB93)), unchecked((int)(0x7262D75C)), 
			unchecked((int)(0x6B79E61D)), unchecked((int)(0x4054B5DE)), unchecked((int)(0x594F849F
			)), unchecked((int)(0x160E1258)), unchecked((int)(0x0F152319)), unchecked((int)(
			0x243870DA)), unchecked((int)(0x3D23419B)), unchecked((int)(0x65FD6BA7)), unchecked(
			(int)(0x7CE65AE6)), unchecked((int)(0x57CB0925)), unchecked((int)(0x4ED03864)), 
			unchecked((int)(0x0191AEA3)), unchecked((int)(0x188A9FE2)), unchecked((int)(0x33A7CC21
			)), unchecked((int)(0x2ABCFD60)), unchecked((int)(0xAD24E1AF)), unchecked((int)(
			0xB43FD0EE)), unchecked((int)(0x9F12832D)), unchecked((int)(0x8609B26C)), unchecked(
			(int)(0xC94824AB)), unchecked((int)(0xD05315EA)), unchecked((int)(0xFB7E4629)), 
			unchecked((int)(0xE2657768)), unchecked((int)(0x2F3F79F6)), unchecked((int)(0x362448B7
			)), unchecked((int)(0x1D091B74)), unchecked((int)(0x04122A35)), unchecked((int)(
			0x4B53BCF2)), unchecked((int)(0x52488DB3)), unchecked((int)(0x7965DE70)), unchecked(
			(int)(0x607EEF31)), unchecked((int)(0xE7E6F3FE)), unchecked((int)(0xFEFDC2BF)), 
			unchecked((int)(0xD5D0917C)), unchecked((int)(0xCCCBA03D)), unchecked((int)(0x838A36FA
			)), unchecked((int)(0x9A9107BB)), unchecked((int)(0xB1BC5478)), unchecked((int)(
			0xA8A76539)), unchecked((int)(0x3B83984B)), unchecked((int)(0x2298A90A)), unchecked(
			(int)(0x09B5FAC9)), unchecked((int)(0x10AECB88)), unchecked((int)(0x5FEF5D4F)), 
			unchecked((int)(0x46F46C0E)), unchecked((int)(0x6DD93FCD)), unchecked((int)(0x74C20E8C
			)), unchecked((int)(0xF35A1243)), unchecked((int)(0xEA412302)), unchecked((int)(
			0xC16C70C1)), unchecked((int)(0xD8774180)), unchecked((int)(0x9736D747)), unchecked(
			(int)(0x8E2DE606)), unchecked((int)(0xA500B5C5)), unchecked((int)(0xBC1B8484)), 
			unchecked((int)(0x71418A1A)), unchecked((int)(0x685ABB5B)), unchecked((int)(0x4377E898
			)), unchecked((int)(0x5A6CD9D9)), unchecked((int)(0x152D4F1E)), unchecked((int)(
			0x0C367E5F)), unchecked((int)(0x271B2D9C)), unchecked((int)(0x3E001CDD)), unchecked(
			(int)(0xB9980012)), unchecked((int)(0xA0833153)), unchecked((int)(0x8BAE6290)), 
			unchecked((int)(0x92B553D1)), unchecked((int)(0xDDF4C516)), unchecked((int)(0xC4EFF457
			)), unchecked((int)(0xEFC2A794)), unchecked((int)(0xF6D996D5)), unchecked((int)(
			0xAE07BCE9)), unchecked((int)(0xB71C8DA8)), unchecked((int)(0x9C31DE6B)), unchecked(
			(int)(0x852AEF2A)), unchecked((int)(0xCA6B79ED)), unchecked((int)(0xD37048AC)), 
			unchecked((int)(0xF85D1B6F)), unchecked((int)(0xE1462A2E)), unchecked((int)(0x66DE36E1
			)), unchecked((int)(0x7FC507A0)), unchecked((int)(0x54E85463)), unchecked((int)(
			0x4DF36522)), unchecked((int)(0x02B2F3E5)), unchecked((int)(0x1BA9C2A4)), unchecked(
			(int)(0x30849167)), unchecked((int)(0x299FA026)), unchecked((int)(0xE4C5AEB8)), 
			unchecked((int)(0xFDDE9FF9)), unchecked((int)(0xD6F3CC3A)), unchecked((int)(0xCFE8FD7B
			)), unchecked((int)(0x80A96BBC)), unchecked((int)(0x99B25AFD)), unchecked((int)(
			0xB29F093E)), unchecked((int)(0xAB84387F)), unchecked((int)(0x2C1C24B0)), unchecked(
			(int)(0x350715F1)), unchecked((int)(0x1E2A4632)), unchecked((int)(0x07317773)), 
			unchecked((int)(0x4870E1B4)), unchecked((int)(0x516BD0F5)), unchecked((int)(0x7A468336
			)), unchecked((int)(0x635DB277)), unchecked((int)(0xCBFAD74E)), unchecked((int)(
			0xD2E1E60F)), unchecked((int)(0xF9CCB5CC)), unchecked((int)(0xE0D7848D)), unchecked(
			(int)(0xAF96124A)), unchecked((int)(0xB68D230B)), unchecked((int)(0x9DA070C8)), 
			unchecked((int)(0x84BB4189)), unchecked((int)(0x03235D46)), unchecked((int)(0x1A386C07
			)), unchecked((int)(0x31153FC4)), unchecked((int)(0x280E0E85)), unchecked((int)(
			0x674F9842)), unchecked((int)(0x7E54A903)), unchecked((int)(0x5579FAC0)), unchecked(
			(int)(0x4C62CB81)), unchecked((int)(0x8138C51F)), unchecked((int)(0x9823F45E)), 
			unchecked((int)(0xB30EA79D)), unchecked((int)(0xAA1596DC)), unchecked((int)(0xE554001B
			)), unchecked((int)(0xFC4F315A)), unchecked((int)(0xD7626299)), unchecked((int)(
			0xCE7953D8)), unchecked((int)(0x49E14F17)), unchecked((int)(0x50FA7E56)), unchecked(
			(int)(0x7BD72D95)), unchecked((int)(0x62CC1CD4)), unchecked((int)(0x2D8D8A13)), 
			unchecked((int)(0x3496BB52)), unchecked((int)(0x1FBBE891)), unchecked((int)(0x06A0D9D0
			)), unchecked((int)(0x5E7EF3EC)), unchecked((int)(0x4765C2AD)), unchecked((int)(
			0x6C48916E)), unchecked((int)(0x7553A02F)), unchecked((int)(0x3A1236E8)), unchecked(
			(int)(0x230907A9)), unchecked((int)(0x0824546A)), unchecked((int)(0x113F652B)), 
			unchecked((int)(0x96A779E4)), unchecked((int)(0x8FBC48A5)), unchecked((int)(0xA4911B66
			)), unchecked((int)(0xBD8A2A27)), unchecked((int)(0xF2CBBCE0)), unchecked((int)(
			0xEBD08DA1)), unchecked((int)(0xC0FDDE62)), unchecked((int)(0xD9E6EF23)), unchecked(
			(int)(0x14BCE1BD)), unchecked((int)(0x0DA7D0FC)), unchecked((int)(0x268A833F)), 
			unchecked((int)(0x3F91B27E)), unchecked((int)(0x70D024B9)), unchecked((int)(0x69CB15F8
			)), unchecked((int)(0x42E6463B)), unchecked((int)(0x5BFD777A)), unchecked((int)(
			0xDC656BB5)), unchecked((int)(0xC57E5AF4)), unchecked((int)(0xEE530937)), unchecked(
			(int)(0xF7483876)), unchecked((int)(0xB809AEB1)), unchecked((int)(0xA1129FF0)), 
			unchecked((int)(0x8A3FCC33)), unchecked((int)(0x9324FD72)), unchecked((int)(0x00000000
			)), unchecked((int)(0x01C26A37)), unchecked((int)(0x0384D46E)), unchecked((int)(
			0x0246BE59)), unchecked((int)(0x0709A8DC)), unchecked((int)(0x06CBC2EB)), unchecked(
			(int)(0x048D7CB2)), unchecked((int)(0x054F1685)), unchecked((int)(0x0E1351B8)), 
			unchecked((int)(0x0FD13B8F)), unchecked((int)(0x0D9785D6)), unchecked((int)(0x0C55EFE1
			)), unchecked((int)(0x091AF964)), unchecked((int)(0x08D89353)), unchecked((int)(
			0x0A9E2D0A)), unchecked((int)(0x0B5C473D)), unchecked((int)(0x1C26A370)), unchecked(
			(int)(0x1DE4C947)), unchecked((int)(0x1FA2771E)), unchecked((int)(0x1E601D29)), 
			unchecked((int)(0x1B2F0BAC)), unchecked((int)(0x1AED619B)), unchecked((int)(0x18ABDFC2
			)), unchecked((int)(0x1969B5F5)), unchecked((int)(0x1235F2C8)), unchecked((int)(
			0x13F798FF)), unchecked((int)(0x11B126A6)), unchecked((int)(0x10734C91)), unchecked(
			(int)(0x153C5A14)), unchecked((int)(0x14FE3023)), unchecked((int)(0x16B88E7A)), 
			unchecked((int)(0x177AE44D)), unchecked((int)(0x384D46E0)), unchecked((int)(0x398F2CD7
			)), unchecked((int)(0x3BC9928E)), unchecked((int)(0x3A0BF8B9)), unchecked((int)(
			0x3F44EE3C)), unchecked((int)(0x3E86840B)), unchecked((int)(0x3CC03A52)), unchecked(
			(int)(0x3D025065)), unchecked((int)(0x365E1758)), unchecked((int)(0x379C7D6F)), 
			unchecked((int)(0x35DAC336)), unchecked((int)(0x3418A901)), unchecked((int)(0x3157BF84
			)), unchecked((int)(0x3095D5B3)), unchecked((int)(0x32D36BEA)), unchecked((int)(
			0x331101DD)), unchecked((int)(0x246BE590)), unchecked((int)(0x25A98FA7)), unchecked(
			(int)(0x27EF31FE)), unchecked((int)(0x262D5BC9)), unchecked((int)(0x23624D4C)), 
			unchecked((int)(0x22A0277B)), unchecked((int)(0x20E69922)), unchecked((int)(0x2124F315
			)), unchecked((int)(0x2A78B428)), unchecked((int)(0x2BBADE1F)), unchecked((int)(
			0x29FC6046)), unchecked((int)(0x283E0A71)), unchecked((int)(0x2D711CF4)), unchecked(
			(int)(0x2CB376C3)), unchecked((int)(0x2EF5C89A)), unchecked((int)(0x2F37A2AD)), 
			unchecked((int)(0x709A8DC0)), unchecked((int)(0x7158E7F7)), unchecked((int)(0x731E59AE
			)), unchecked((int)(0x72DC3399)), unchecked((int)(0x7793251C)), unchecked((int)(
			0x76514F2B)), unchecked((int)(0x7417F172)), unchecked((int)(0x75D59B45)), unchecked(
			(int)(0x7E89DC78)), unchecked((int)(0x7F4BB64F)), unchecked((int)(0x7D0D0816)), 
			unchecked((int)(0x7CCF6221)), unchecked((int)(0x798074A4)), unchecked((int)(0x78421E93
			)), unchecked((int)(0x7A04A0CA)), unchecked((int)(0x7BC6CAFD)), unchecked((int)(
			0x6CBC2EB0)), unchecked((int)(0x6D7E4487)), unchecked((int)(0x6F38FADE)), unchecked(
			(int)(0x6EFA90E9)), unchecked((int)(0x6BB5866C)), unchecked((int)(0x6A77EC5B)), 
			unchecked((int)(0x68315202)), unchecked((int)(0x69F33835)), unchecked((int)(0x62AF7F08
			)), unchecked((int)(0x636D153F)), unchecked((int)(0x612BAB66)), unchecked((int)(
			0x60E9C151)), unchecked((int)(0x65A6D7D4)), unchecked((int)(0x6464BDE3)), unchecked(
			(int)(0x662203BA)), unchecked((int)(0x67E0698D)), unchecked((int)(0x48D7CB20)), 
			unchecked((int)(0x4915A117)), unchecked((int)(0x4B531F4E)), unchecked((int)(0x4A917579
			)), unchecked((int)(0x4FDE63FC)), unchecked((int)(0x4E1C09CB)), unchecked((int)(
			0x4C5AB792)), unchecked((int)(0x4D98DDA5)), unchecked((int)(0x46C49A98)), unchecked(
			(int)(0x4706F0AF)), unchecked((int)(0x45404EF6)), unchecked((int)(0x448224C1)), 
			unchecked((int)(0x41CD3244)), unchecked((int)(0x400F5873)), unchecked((int)(0x4249E62A
			)), unchecked((int)(0x438B8C1D)), unchecked((int)(0x54F16850)), unchecked((int)(
			0x55330267)), unchecked((int)(0x5775BC3E)), unchecked((int)(0x56B7D609)), unchecked(
			(int)(0x53F8C08C)), unchecked((int)(0x523AAABB)), unchecked((int)(0x507C14E2)), 
			unchecked((int)(0x51BE7ED5)), unchecked((int)(0x5AE239E8)), unchecked((int)(0x5B2053DF
			)), unchecked((int)(0x5966ED86)), unchecked((int)(0x58A487B1)), unchecked((int)(
			0x5DEB9134)), unchecked((int)(0x5C29FB03)), unchecked((int)(0x5E6F455A)), unchecked(
			(int)(0x5FAD2F6D)), unchecked((int)(0xE1351B80)), unchecked((int)(0xE0F771B7)), 
			unchecked((int)(0xE2B1CFEE)), unchecked((int)(0xE373A5D9)), unchecked((int)(0xE63CB35C
			)), unchecked((int)(0xE7FED96B)), unchecked((int)(0xE5B86732)), unchecked((int)(
			0xE47A0D05)), unchecked((int)(0xEF264A38)), unchecked((int)(0xEEE4200F)), unchecked(
			(int)(0xECA29E56)), unchecked((int)(0xED60F461)), unchecked((int)(0xE82FE2E4)), 
			unchecked((int)(0xE9ED88D3)), unchecked((int)(0xEBAB368A)), unchecked((int)(0xEA695CBD
			)), unchecked((int)(0xFD13B8F0)), unchecked((int)(0xFCD1D2C7)), unchecked((int)(
			0xFE976C9E)), unchecked((int)(0xFF5506A9)), unchecked((int)(0xFA1A102C)), unchecked(
			(int)(0xFBD87A1B)), unchecked((int)(0xF99EC442)), unchecked((int)(0xF85CAE75)), 
			unchecked((int)(0xF300E948)), unchecked((int)(0xF2C2837F)), unchecked((int)(0xF0843D26
			)), unchecked((int)(0xF1465711)), unchecked((int)(0xF4094194)), unchecked((int)(
			0xF5CB2BA3)), unchecked((int)(0xF78D95FA)), unchecked((int)(0xF64FFFCD)), unchecked(
			(int)(0xD9785D60)), unchecked((int)(0xD8BA3757)), unchecked((int)(0xDAFC890E)), 
			unchecked((int)(0xDB3EE339)), unchecked((int)(0xDE71F5BC)), unchecked((int)(0xDFB39F8B
			)), unchecked((int)(0xDDF521D2)), unchecked((int)(0xDC374BE5)), unchecked((int)(
			0xD76B0CD8)), unchecked((int)(0xD6A966EF)), unchecked((int)(0xD4EFD8B6)), unchecked(
			(int)(0xD52DB281)), unchecked((int)(0xD062A404)), unchecked((int)(0xD1A0CE33)), 
			unchecked((int)(0xD3E6706A)), unchecked((int)(0xD2241A5D)), unchecked((int)(0xC55EFE10
			)), unchecked((int)(0xC49C9427)), unchecked((int)(0xC6DA2A7E)), unchecked((int)(
			0xC7184049)), unchecked((int)(0xC25756CC)), unchecked((int)(0xC3953CFB)), unchecked(
			(int)(0xC1D382A2)), unchecked((int)(0xC011E895)), unchecked((int)(0xCB4DAFA8)), 
			unchecked((int)(0xCA8FC59F)), unchecked((int)(0xC8C97BC6)), unchecked((int)(0xC90B11F1
			)), unchecked((int)(0xCC440774)), unchecked((int)(0xCD866D43)), unchecked((int)(
			0xCFC0D31A)), unchecked((int)(0xCE02B92D)), unchecked((int)(0x91AF9640)), unchecked(
			(int)(0x906DFC77)), unchecked((int)(0x922B422E)), unchecked((int)(0x93E92819)), 
			unchecked((int)(0x96A63E9C)), unchecked((int)(0x976454AB)), unchecked((int)(0x9522EAF2
			)), unchecked((int)(0x94E080C5)), unchecked((int)(0x9FBCC7F8)), unchecked((int)(
			0x9E7EADCF)), unchecked((int)(0x9C381396)), unchecked((int)(0x9DFA79A1)), unchecked(
			(int)(0x98B56F24)), unchecked((int)(0x99770513)), unchecked((int)(0x9B31BB4A)), 
			unchecked((int)(0x9AF3D17D)), unchecked((int)(0x8D893530)), unchecked((int)(0x8C4B5F07
			)), unchecked((int)(0x8E0DE15E)), unchecked((int)(0x8FCF8B69)), unchecked((int)(
			0x8A809DEC)), unchecked((int)(0x8B42F7DB)), unchecked((int)(0x89044982)), unchecked(
			(int)(0x88C623B5)), unchecked((int)(0x839A6488)), unchecked((int)(0x82580EBF)), 
			unchecked((int)(0x801EB0E6)), unchecked((int)(0x81DCDAD1)), unchecked((int)(0x8493CC54
			)), unchecked((int)(0x8551A663)), unchecked((int)(0x8717183A)), unchecked((int)(
			0x86D5720D)), unchecked((int)(0xA9E2D0A0)), unchecked((int)(0xA820BA97)), unchecked(
			(int)(0xAA6604CE)), unchecked((int)(0xABA46EF9)), unchecked((int)(0xAEEB787C)), 
			unchecked((int)(0xAF29124B)), unchecked((int)(0xAD6FAC12)), unchecked((int)(0xACADC625
			)), unchecked((int)(0xA7F18118)), unchecked((int)(0xA633EB2F)), unchecked((int)(
			0xA4755576)), unchecked((int)(0xA5B73F41)), unchecked((int)(0xA0F829C4)), unchecked(
			(int)(0xA13A43F3)), unchecked((int)(0xA37CFDAA)), unchecked((int)(0xA2BE979D)), 
			unchecked((int)(0xB5C473D0)), unchecked((int)(0xB40619E7)), unchecked((int)(0xB640A7BE
			)), unchecked((int)(0xB782CD89)), unchecked((int)(0xB2CDDB0C)), unchecked((int)(
			0xB30FB13B)), unchecked((int)(0xB1490F62)), unchecked((int)(0xB08B6555)), unchecked(
			(int)(0xBBD72268)), unchecked((int)(0xBA15485F)), unchecked((int)(0xB853F606)), 
			unchecked((int)(0xB9919C31)), unchecked((int)(0xBCDE8AB4)), unchecked((int)(0xBD1CE083
			)), unchecked((int)(0xBF5A5EDA)), unchecked((int)(0xBE9834ED)), unchecked((int)(
			0x00000000)), unchecked((int)(0xB8BC6765)), unchecked((int)(0xAA09C88B)), unchecked(
			(int)(0x12B5AFEE)), unchecked((int)(0x8F629757)), unchecked((int)(0x37DEF032)), 
			unchecked((int)(0x256B5FDC)), unchecked((int)(0x9DD738B9)), unchecked((int)(0xC5B428EF
			)), unchecked((int)(0x7D084F8A)), unchecked((int)(0x6FBDE064)), unchecked((int)(
			0xD7018701)), unchecked((int)(0x4AD6BFB8)), unchecked((int)(0xF26AD8DD)), unchecked(
			(int)(0xE0DF7733)), unchecked((int)(0x58631056)), unchecked((int)(0x5019579F)), 
			unchecked((int)(0xE8A530FA)), unchecked((int)(0xFA109F14)), unchecked((int)(0x42ACF871
			)), unchecked((int)(0xDF7BC0C8)), unchecked((int)(0x67C7A7AD)), unchecked((int)(
			0x75720843)), unchecked((int)(0xCDCE6F26)), unchecked((int)(0x95AD7F70)), unchecked(
			(int)(0x2D111815)), unchecked((int)(0x3FA4B7FB)), unchecked((int)(0x8718D09E)), 
			unchecked((int)(0x1ACFE827)), unchecked((int)(0xA2738F42)), unchecked((int)(0xB0C620AC
			)), unchecked((int)(0x087A47C9)), unchecked((int)(0xA032AF3E)), unchecked((int)(
			0x188EC85B)), unchecked((int)(0x0A3B67B5)), unchecked((int)(0xB28700D0)), unchecked(
			(int)(0x2F503869)), unchecked((int)(0x97EC5F0C)), unchecked((int)(0x8559F0E2)), 
			unchecked((int)(0x3DE59787)), unchecked((int)(0x658687D1)), unchecked((int)(0xDD3AE0B4
			)), unchecked((int)(0xCF8F4F5A)), unchecked((int)(0x7733283F)), unchecked((int)(
			0xEAE41086)), unchecked((int)(0x525877E3)), unchecked((int)(0x40EDD80D)), unchecked(
			(int)(0xF851BF68)), unchecked((int)(0xF02BF8A1)), unchecked((int)(0x48979FC4)), 
			unchecked((int)(0x5A22302A)), unchecked((int)(0xE29E574F)), unchecked((int)(0x7F496FF6
			)), unchecked((int)(0xC7F50893)), unchecked((int)(0xD540A77D)), unchecked((int)(
			0x6DFCC018)), unchecked((int)(0x359FD04E)), unchecked((int)(0x8D23B72B)), unchecked(
			(int)(0x9F9618C5)), unchecked((int)(0x272A7FA0)), unchecked((int)(0xBAFD4719)), 
			unchecked((int)(0x0241207C)), unchecked((int)(0x10F48F92)), unchecked((int)(0xA848E8F7
			)), unchecked((int)(0x9B14583D)), unchecked((int)(0x23A83F58)), unchecked((int)(
			0x311D90B6)), unchecked((int)(0x89A1F7D3)), unchecked((int)(0x1476CF6A)), unchecked(
			(int)(0xACCAA80F)), unchecked((int)(0xBE7F07E1)), unchecked((int)(0x06C36084)), 
			unchecked((int)(0x5EA070D2)), unchecked((int)(0xE61C17B7)), unchecked((int)(0xF4A9B859
			)), unchecked((int)(0x4C15DF3C)), unchecked((int)(0xD1C2E785)), unchecked((int)(
			0x697E80E0)), unchecked((int)(0x7BCB2F0E)), unchecked((int)(0xC377486B)), unchecked(
			(int)(0xCB0D0FA2)), unchecked((int)(0x73B168C7)), unchecked((int)(0x6104C729)), 
			unchecked((int)(0xD9B8A04C)), unchecked((int)(0x446F98F5)), unchecked((int)(0xFCD3FF90
			)), unchecked((int)(0xEE66507E)), unchecked((int)(0x56DA371B)), unchecked((int)(
			0x0EB9274D)), unchecked((int)(0xB6054028)), unchecked((int)(0xA4B0EFC6)), unchecked(
			(int)(0x1C0C88A3)), unchecked((int)(0x81DBB01A)), unchecked((int)(0x3967D77F)), 
			unchecked((int)(0x2BD27891)), unchecked((int)(0x936E1FF4)), unchecked((int)(0x3B26F703
			)), unchecked((int)(0x839A9066)), unchecked((int)(0x912F3F88)), unchecked((int)(
			0x299358ED)), unchecked((int)(0xB4446054)), unchecked((int)(0x0CF80731)), unchecked(
			(int)(0x1E4DA8DF)), unchecked((int)(0xA6F1CFBA)), unchecked((int)(0xFE92DFEC)), 
			unchecked((int)(0x462EB889)), unchecked((int)(0x549B1767)), unchecked((int)(0xEC277002
			)), unchecked((int)(0x71F048BB)), unchecked((int)(0xC94C2FDE)), unchecked((int)(
			0xDBF98030)), unchecked((int)(0x6345E755)), unchecked((int)(0x6B3FA09C)), unchecked(
			(int)(0xD383C7F9)), unchecked((int)(0xC1366817)), unchecked((int)(0x798A0F72)), 
			unchecked((int)(0xE45D37CB)), unchecked((int)(0x5CE150AE)), unchecked((int)(0x4E54FF40
			)), unchecked((int)(0xF6E89825)), unchecked((int)(0xAE8B8873)), unchecked((int)(
			0x1637EF16)), unchecked((int)(0x048240F8)), unchecked((int)(0xBC3E279D)), unchecked(
			(int)(0x21E91F24)), unchecked((int)(0x99557841)), unchecked((int)(0x8BE0D7AF)), 
			unchecked((int)(0x335CB0CA)), unchecked((int)(0xED59B63B)), unchecked((int)(0x55E5D15E
			)), unchecked((int)(0x47507EB0)), unchecked((int)(0xFFEC19D5)), unchecked((int)(
			0x623B216C)), unchecked((int)(0xDA874609)), unchecked((int)(0xC832E9E7)), unchecked(
			(int)(0x708E8E82)), unchecked((int)(0x28ED9ED4)), unchecked((int)(0x9051F9B1)), 
			unchecked((int)(0x82E4565F)), unchecked((int)(0x3A58313A)), unchecked((int)(0xA78F0983
			)), unchecked((int)(0x1F336EE6)), unchecked((int)(0x0D86C108)), unchecked((int)(
			0xB53AA66D)), unchecked((int)(0xBD40E1A4)), unchecked((int)(0x05FC86C1)), unchecked(
			(int)(0x1749292F)), unchecked((int)(0xAFF54E4A)), unchecked((int)(0x322276F3)), 
			unchecked((int)(0x8A9E1196)), unchecked((int)(0x982BBE78)), unchecked((int)(0x2097D91D
			)), unchecked((int)(0x78F4C94B)), unchecked((int)(0xC048AE2E)), unchecked((int)(
			0xD2FD01C0)), unchecked((int)(0x6A4166A5)), unchecked((int)(0xF7965E1C)), unchecked(
			(int)(0x4F2A3979)), unchecked((int)(0x5D9F9697)), unchecked((int)(0xE523F1F2)), 
			unchecked((int)(0x4D6B1905)), unchecked((int)(0xF5D77E60)), unchecked((int)(0xE762D18E
			)), unchecked((int)(0x5FDEB6EB)), unchecked((int)(0xC2098E52)), unchecked((int)(
			0x7AB5E937)), unchecked((int)(0x680046D9)), unchecked((int)(0xD0BC21BC)), unchecked(
			(int)(0x88DF31EA)), unchecked((int)(0x3063568F)), unchecked((int)(0x22D6F961)), 
			unchecked((int)(0x9A6A9E04)), unchecked((int)(0x07BDA6BD)), unchecked((int)(0xBF01C1D8
			)), unchecked((int)(0xADB46E36)), unchecked((int)(0x15080953)), unchecked((int)(
			0x1D724E9A)), unchecked((int)(0xA5CE29FF)), unchecked((int)(0xB77B8611)), unchecked(
			(int)(0x0FC7E174)), unchecked((int)(0x9210D9CD)), unchecked((int)(0x2AACBEA8)), 
			unchecked((int)(0x38191146)), unchecked((int)(0x80A57623)), unchecked((int)(0xD8C66675
			)), unchecked((int)(0x607A0110)), unchecked((int)(0x72CFAEFE)), unchecked((int)(
			0xCA73C99B)), unchecked((int)(0x57A4F122)), unchecked((int)(0xEF189647)), unchecked(
			(int)(0xFDAD39A9)), unchecked((int)(0x45115ECC)), unchecked((int)(0x764DEE06)), 
			unchecked((int)(0xCEF18963)), unchecked((int)(0xDC44268D)), unchecked((int)(0x64F841E8
			)), unchecked((int)(0xF92F7951)), unchecked((int)(0x41931E34)), unchecked((int)(
			0x5326B1DA)), unchecked((int)(0xEB9AD6BF)), unchecked((int)(0xB3F9C6E9)), unchecked(
			(int)(0x0B45A18C)), unchecked((int)(0x19F00E62)), unchecked((int)(0xA14C6907)), 
			unchecked((int)(0x3C9B51BE)), unchecked((int)(0x842736DB)), unchecked((int)(0x96929935
			)), unchecked((int)(0x2E2EFE50)), unchecked((int)(0x2654B999)), unchecked((int)(
			0x9EE8DEFC)), unchecked((int)(0x8C5D7112)), unchecked((int)(0x34E11677)), unchecked(
			(int)(0xA9362ECE)), unchecked((int)(0x118A49AB)), unchecked((int)(0x033FE645)), 
			unchecked((int)(0xBB838120)), unchecked((int)(0xE3E09176)), unchecked((int)(0x5B5CF613
			)), unchecked((int)(0x49E959FD)), unchecked((int)(0xF1553E98)), unchecked((int)(
			0x6C820621)), unchecked((int)(0xD43E6144)), unchecked((int)(0xC68BCEAA)), unchecked(
			(int)(0x7E37A9CF)), unchecked((int)(0xD67F4138)), unchecked((int)(0x6EC3265D)), 
			unchecked((int)(0x7C7689B3)), unchecked((int)(0xC4CAEED6)), unchecked((int)(0x591DD66F
			)), unchecked((int)(0xE1A1B10A)), unchecked((int)(0xF3141EE4)), unchecked((int)(
			0x4BA87981)), unchecked((int)(0x13CB69D7)), unchecked((int)(0xAB770EB2)), unchecked(
			(int)(0xB9C2A15C)), unchecked((int)(0x017EC639)), unchecked((int)(0x9CA9FE80)), 
			unchecked((int)(0x241599E5)), unchecked((int)(0x36A0360B)), unchecked((int)(0x8E1C516E
			)), unchecked((int)(0x866616A7)), unchecked((int)(0x3EDA71C2)), unchecked((int)(
			0x2C6FDE2C)), unchecked((int)(0x94D3B949)), unchecked((int)(0x090481F0)), unchecked(
			(int)(0xB1B8E695)), unchecked((int)(0xA30D497B)), unchecked((int)(0x1BB12E1E)), 
			unchecked((int)(0x43D23E48)), unchecked((int)(0xFB6E592D)), unchecked((int)(0xE9DBF6C3
			)), unchecked((int)(0x516791A6)), unchecked((int)(0xCCB0A91F)), unchecked((int)(
			0x740CCE7A)), unchecked((int)(0x66B96194)), unchecked((int)(0xDE0506F1)), unchecked(
			(int)(0x00000000)), unchecked((int)(0x3D6029B0)), unchecked((int)(0x7AC05360)), 
			unchecked((int)(0x47A07AD0)), unchecked((int)(0xF580A6C0)), unchecked((int)(0xC8E08F70
			)), unchecked((int)(0x8F40F5A0)), unchecked((int)(0xB220DC10)), unchecked((int)(
			0x30704BC1)), unchecked((int)(0x0D106271)), unchecked((int)(0x4AB018A1)), unchecked(
			(int)(0x77D03111)), unchecked((int)(0xC5F0ED01)), unchecked((int)(0xF890C4B1)), 
			unchecked((int)(0xBF30BE61)), unchecked((int)(0x825097D1)), unchecked((int)(0x60E09782
			)), unchecked((int)(0x5D80BE32)), unchecked((int)(0x1A20C4E2)), unchecked((int)(
			0x2740ED52)), unchecked((int)(0x95603142)), unchecked((int)(0xA80018F2)), unchecked(
			(int)(0xEFA06222)), unchecked((int)(0xD2C04B92)), unchecked((int)(0x5090DC43)), 
			unchecked((int)(0x6DF0F5F3)), unchecked((int)(0x2A508F23)), unchecked((int)(0x1730A693
			)), unchecked((int)(0xA5107A83)), unchecked((int)(0x98705333)), unchecked((int)(
			0xDFD029E3)), unchecked((int)(0xE2B00053)), unchecked((int)(0xC1C12F04)), unchecked(
			(int)(0xFCA106B4)), unchecked((int)(0xBB017C64)), unchecked((int)(0x866155D4)), 
			unchecked((int)(0x344189C4)), unchecked((int)(0x0921A074)), unchecked((int)(0x4E81DAA4
			)), unchecked((int)(0x73E1F314)), unchecked((int)(0xF1B164C5)), unchecked((int)(
			0xCCD14D75)), unchecked((int)(0x8B7137A5)), unchecked((int)(0xB6111E15)), unchecked(
			(int)(0x0431C205)), unchecked((int)(0x3951EBB5)), unchecked((int)(0x7EF19165)), 
			unchecked((int)(0x4391B8D5)), unchecked((int)(0xA121B886)), unchecked((int)(0x9C419136
			)), unchecked((int)(0xDBE1EBE6)), unchecked((int)(0xE681C256)), unchecked((int)(
			0x54A11E46)), unchecked((int)(0x69C137F6)), unchecked((int)(0x2E614D26)), unchecked(
			(int)(0x13016496)), unchecked((int)(0x9151F347)), unchecked((int)(0xAC31DAF7)), 
			unchecked((int)(0xEB91A027)), unchecked((int)(0xD6F18997)), unchecked((int)(0x64D15587
			)), unchecked((int)(0x59B17C37)), unchecked((int)(0x1E1106E7)), unchecked((int)(
			0x23712F57)), unchecked((int)(0x58F35849)), unchecked((int)(0x659371F9)), unchecked(
			(int)(0x22330B29)), unchecked((int)(0x1F532299)), unchecked((int)(0xAD73FE89)), 
			unchecked((int)(0x9013D739)), unchecked((int)(0xD7B3ADE9)), unchecked((int)(0xEAD38459
			)), unchecked((int)(0x68831388)), unchecked((int)(0x55E33A38)), unchecked((int)(
			0x124340E8)), unchecked((int)(0x2F236958)), unchecked((int)(0x9D03B548)), unchecked(
			(int)(0xA0639CF8)), unchecked((int)(0xE7C3E628)), unchecked((int)(0xDAA3CF98)), 
			unchecked((int)(0x3813CFCB)), unchecked((int)(0x0573E67B)), unchecked((int)(0x42D39CAB
			)), unchecked((int)(0x7FB3B51B)), unchecked((int)(0xCD93690B)), unchecked((int)(
			0xF0F340BB)), unchecked((int)(0xB7533A6B)), unchecked((int)(0x8A3313DB)), unchecked(
			(int)(0x0863840A)), unchecked((int)(0x3503ADBA)), unchecked((int)(0x72A3D76A)), 
			unchecked((int)(0x4FC3FEDA)), unchecked((int)(0xFDE322CA)), unchecked((int)(0xC0830B7A
			)), unchecked((int)(0x872371AA)), unchecked((int)(0xBA43581A)), unchecked((int)(
			0x9932774D)), unchecked((int)(0xA4525EFD)), unchecked((int)(0xE3F2242D)), unchecked(
			(int)(0xDE920D9D)), unchecked((int)(0x6CB2D18D)), unchecked((int)(0x51D2F83D)), 
			unchecked((int)(0x167282ED)), unchecked((int)(0x2B12AB5D)), unchecked((int)(0xA9423C8C
			)), unchecked((int)(0x9422153C)), unchecked((int)(0xD3826FEC)), unchecked((int)(
			0xEEE2465C)), unchecked((int)(0x5CC29A4C)), unchecked((int)(0x61A2B3FC)), unchecked(
			(int)(0x2602C92C)), unchecked((int)(0x1B62E09C)), unchecked((int)(0xF9D2E0CF)), 
			unchecked((int)(0xC4B2C97F)), unchecked((int)(0x8312B3AF)), unchecked((int)(0xBE729A1F
			)), unchecked((int)(0x0C52460F)), unchecked((int)(0x31326FBF)), unchecked((int)(
			0x7692156F)), unchecked((int)(0x4BF23CDF)), unchecked((int)(0xC9A2AB0E)), unchecked(
			(int)(0xF4C282BE)), unchecked((int)(0xB362F86E)), unchecked((int)(0x8E02D1DE)), 
			unchecked((int)(0x3C220DCE)), unchecked((int)(0x0142247E)), unchecked((int)(0x46E25EAE
			)), unchecked((int)(0x7B82771E)), unchecked((int)(0xB1E6B092)), unchecked((int)(
			0x8C869922)), unchecked((int)(0xCB26E3F2)), unchecked((int)(0xF646CA42)), unchecked(
			(int)(0x44661652)), unchecked((int)(0x79063FE2)), unchecked((int)(0x3EA64532)), 
			unchecked((int)(0x03C66C82)), unchecked((int)(0x8196FB53)), unchecked((int)(0xBCF6D2E3
			)), unchecked((int)(0xFB56A833)), unchecked((int)(0xC6368183)), unchecked((int)(
			0x74165D93)), unchecked((int)(0x49767423)), unchecked((int)(0x0ED60EF3)), unchecked(
			(int)(0x33B62743)), unchecked((int)(0xD1062710)), unchecked((int)(0xEC660EA0)), 
			unchecked((int)(0xABC67470)), unchecked((int)(0x96A65DC0)), unchecked((int)(0x248681D0
			)), unchecked((int)(0x19E6A860)), unchecked((int)(0x5E46D2B0)), unchecked((int)(
			0x6326FB00)), unchecked((int)(0xE1766CD1)), unchecked((int)(0xDC164561)), unchecked(
			(int)(0x9BB63FB1)), unchecked((int)(0xA6D61601)), unchecked((int)(0x14F6CA11)), 
			unchecked((int)(0x2996E3A1)), unchecked((int)(0x6E369971)), unchecked((int)(0x5356B0C1
			)), unchecked((int)(0x70279F96)), unchecked((int)(0x4D47B626)), unchecked((int)(
			0x0AE7CCF6)), unchecked((int)(0x3787E546)), unchecked((int)(0x85A73956)), unchecked(
			(int)(0xB8C710E6)), unchecked((int)(0xFF676A36)), unchecked((int)(0xC2074386)), 
			unchecked((int)(0x4057D457)), unchecked((int)(0x7D37FDE7)), unchecked((int)(0x3A978737
			)), unchecked((int)(0x07F7AE87)), unchecked((int)(0xB5D77297)), unchecked((int)(
			0x88B75B27)), unchecked((int)(0xCF1721F7)), unchecked((int)(0xF2770847)), unchecked(
			(int)(0x10C70814)), unchecked((int)(0x2DA721A4)), unchecked((int)(0x6A075B74)), 
			unchecked((int)(0x576772C4)), unchecked((int)(0xE547AED4)), unchecked((int)(0xD8278764
			)), unchecked((int)(0x9F87FDB4)), unchecked((int)(0xA2E7D404)), unchecked((int)(
			0x20B743D5)), unchecked((int)(0x1DD76A65)), unchecked((int)(0x5A7710B5)), unchecked(
			(int)(0x67173905)), unchecked((int)(0xD537E515)), unchecked((int)(0xE857CCA5)), 
			unchecked((int)(0xAFF7B675)), unchecked((int)(0x92979FC5)), unchecked((int)(0xE915E8DB
			)), unchecked((int)(0xD475C16B)), unchecked((int)(0x93D5BBBB)), unchecked((int)(
			0xAEB5920B)), unchecked((int)(0x1C954E1B)), unchecked((int)(0x21F567AB)), unchecked(
			(int)(0x66551D7B)), unchecked((int)(0x5B3534CB)), unchecked((int)(0xD965A31A)), 
			unchecked((int)(0xE4058AAA)), unchecked((int)(0xA3A5F07A)), unchecked((int)(0x9EC5D9CA
			)), unchecked((int)(0x2CE505DA)), unchecked((int)(0x11852C6A)), unchecked((int)(
			0x562556BA)), unchecked((int)(0x6B457F0A)), unchecked((int)(0x89F57F59)), unchecked(
			(int)(0xB49556E9)), unchecked((int)(0xF3352C39)), unchecked((int)(0xCE550589)), 
			unchecked((int)(0x7C75D999)), unchecked((int)(0x4115F029)), unchecked((int)(0x06B58AF9
			)), unchecked((int)(0x3BD5A349)), unchecked((int)(0xB9853498)), unchecked((int)(
			0x84E51D28)), unchecked((int)(0xC34567F8)), unchecked((int)(0xFE254E48)), unchecked(
			(int)(0x4C059258)), unchecked((int)(0x7165BBE8)), unchecked((int)(0x36C5C138)), 
			unchecked((int)(0x0BA5E888)), unchecked((int)(0x28D4C7DF)), unchecked((int)(0x15B4EE6F
			)), unchecked((int)(0x521494BF)), unchecked((int)(0x6F74BD0F)), unchecked((int)(
			0xDD54611F)), unchecked((int)(0xE03448AF)), unchecked((int)(0xA794327F)), unchecked(
			(int)(0x9AF41BCF)), unchecked((int)(0x18A48C1E)), unchecked((int)(0x25C4A5AE)), 
			unchecked((int)(0x6264DF7E)), unchecked((int)(0x5F04F6CE)), unchecked((int)(0xED242ADE
			)), unchecked((int)(0xD044036E)), unchecked((int)(0x97E479BE)), unchecked((int)(
			0xAA84500E)), unchecked((int)(0x4834505D)), unchecked((int)(0x755479ED)), unchecked(
			(int)(0x32F4033D)), unchecked((int)(0x0F942A8D)), unchecked((int)(0xBDB4F69D)), 
			unchecked((int)(0x80D4DF2D)), unchecked((int)(0xC774A5FD)), unchecked((int)(0xFA148C4D
			)), unchecked((int)(0x78441B9C)), unchecked((int)(0x4524322C)), unchecked((int)(
			0x028448FC)), unchecked((int)(0x3FE4614C)), unchecked((int)(0x8DC4BD5C)), unchecked(
			(int)(0xB0A494EC)), unchecked((int)(0xF704EE3C)), unchecked((int)(0xCA64C78C)), 
			unchecked((int)(0x00000000)), unchecked((int)(0xCB5CD3A5)), unchecked((int)(0x4DC8A10B
			)), unchecked((int)(0x869472AE)), unchecked((int)(0x9B914216)), unchecked((int)(
			0x50CD91B3)), unchecked((int)(0xD659E31D)), unchecked((int)(0x1D0530B8)), unchecked(
			(int)(0xEC53826D)), unchecked((int)(0x270F51C8)), unchecked((int)(0xA19B2366)), 
			unchecked((int)(0x6AC7F0C3)), unchecked((int)(0x77C2C07B)), unchecked((int)(0xBC9E13DE
			)), unchecked((int)(0x3A0A6170)), unchecked((int)(0xF156B2D5)), unchecked((int)(
			0x03D6029B)), unchecked((int)(0xC88AD13E)), unchecked((int)(0x4E1EA390)), unchecked(
			(int)(0x85427035)), unchecked((int)(0x9847408D)), unchecked((int)(0x531B9328)), 
			unchecked((int)(0xD58FE186)), unchecked((int)(0x1ED33223)), unchecked((int)(0xEF8580F6
			)), unchecked((int)(0x24D95353)), unchecked((int)(0xA24D21FD)), unchecked((int)(
			0x6911F258)), unchecked((int)(0x7414C2E0)), unchecked((int)(0xBF481145)), unchecked(
			(int)(0x39DC63EB)), unchecked((int)(0xF280B04E)), unchecked((int)(0x07AC0536)), 
			unchecked((int)(0xCCF0D693)), unchecked((int)(0x4A64A43D)), unchecked((int)(0x81387798
			)), unchecked((int)(0x9C3D4720)), unchecked((int)(0x57619485)), unchecked((int)(
			0xD1F5E62B)), unchecked((int)(0x1AA9358E)), unchecked((int)(0xEBFF875B)), unchecked(
			(int)(0x20A354FE)), unchecked((int)(0xA6372650)), unchecked((int)(0x6D6BF5F5)), 
			unchecked((int)(0x706EC54D)), unchecked((int)(0xBB3216E8)), unchecked((int)(0x3DA66446
			)), unchecked((int)(0xF6FAB7E3)), unchecked((int)(0x047A07AD)), unchecked((int)(
			0xCF26D408)), unchecked((int)(0x49B2A6A6)), unchecked((int)(0x82EE7503)), unchecked(
			(int)(0x9FEB45BB)), unchecked((int)(0x54B7961E)), unchecked((int)(0xD223E4B0)), 
			unchecked((int)(0x197F3715)), unchecked((int)(0xE82985C0)), unchecked((int)(0x23755665
			)), unchecked((int)(0xA5E124CB)), unchecked((int)(0x6EBDF76E)), unchecked((int)(
			0x73B8C7D6)), unchecked((int)(0xB8E41473)), unchecked((int)(0x3E7066DD)), unchecked(
			(int)(0xF52CB578)), unchecked((int)(0x0F580A6C)), unchecked((int)(0xC404D9C9)), 
			unchecked((int)(0x4290AB67)), unchecked((int)(0x89CC78C2)), unchecked((int)(0x94C9487A
			)), unchecked((int)(0x5F959BDF)), unchecked((int)(0xD901E971)), unchecked((int)(
			0x125D3AD4)), unchecked((int)(0xE30B8801)), unchecked((int)(0x28575BA4)), unchecked(
			(int)(0xAEC3290A)), unchecked((int)(0x659FFAAF)), unchecked((int)(0x789ACA17)), 
			unchecked((int)(0xB3C619B2)), unchecked((int)(0x35526B1C)), unchecked((int)(0xFE0EB8B9
			)), unchecked((int)(0x0C8E08F7)), unchecked((int)(0xC7D2DB52)), unchecked((int)(
			0x4146A9FC)), unchecked((int)(0x8A1A7A59)), unchecked((int)(0x971F4AE1)), unchecked(
			(int)(0x5C439944)), unchecked((int)(0xDAD7EBEA)), unchecked((int)(0x118B384F)), 
			unchecked((int)(0xE0DD8A9A)), unchecked((int)(0x2B81593F)), unchecked((int)(0xAD152B91
			)), unchecked((int)(0x6649F834)), unchecked((int)(0x7B4CC88C)), unchecked((int)(
			0xB0101B29)), unchecked((int)(0x36846987)), unchecked((int)(0xFDD8BA22)), unchecked(
			(int)(0x08F40F5A)), unchecked((int)(0xC3A8DCFF)), unchecked((int)(0x453CAE51)), 
			unchecked((int)(0x8E607DF4)), unchecked((int)(0x93654D4C)), unchecked((int)(0x58399EE9
			)), unchecked((int)(0xDEADEC47)), unchecked((int)(0x15F13FE2)), unchecked((int)(
			0xE4A78D37)), unchecked((int)(0x2FFB5E92)), unchecked((int)(0xA96F2C3C)), unchecked(
			(int)(0x6233FF99)), unchecked((int)(0x7F36CF21)), unchecked((int)(0xB46A1C84)), 
			unchecked((int)(0x32FE6E2A)), unchecked((int)(0xF9A2BD8F)), unchecked((int)(0x0B220DC1
			)), unchecked((int)(0xC07EDE64)), unchecked((int)(0x46EAACCA)), unchecked((int)(
			0x8DB67F6F)), unchecked((int)(0x90B34FD7)), unchecked((int)(0x5BEF9C72)), unchecked(
			(int)(0xDD7BEEDC)), unchecked((int)(0x16273D79)), unchecked((int)(0xE7718FAC)), 
			unchecked((int)(0x2C2D5C09)), unchecked((int)(0xAAB92EA7)), unchecked((int)(0x61E5FD02
			)), unchecked((int)(0x7CE0CDBA)), unchecked((int)(0xB7BC1E1F)), unchecked((int)(
			0x31286CB1)), unchecked((int)(0xFA74BF14)), unchecked((int)(0x1EB014D8)), unchecked(
			(int)(0xD5ECC77D)), unchecked((int)(0x5378B5D3)), unchecked((int)(0x98246676)), 
			unchecked((int)(0x852156CE)), unchecked((int)(0x4E7D856B)), unchecked((int)(0xC8E9F7C5
			)), unchecked((int)(0x03B52460)), unchecked((int)(0xF2E396B5)), unchecked((int)(
			0x39BF4510)), unchecked((int)(0xBF2B37BE)), unchecked((int)(0x7477E41B)), unchecked(
			(int)(0x6972D4A3)), unchecked((int)(0xA22E0706)), unchecked((int)(0x24BA75A8)), 
			unchecked((int)(0xEFE6A60D)), unchecked((int)(0x1D661643)), unchecked((int)(0xD63AC5E6
			)), unchecked((int)(0x50AEB748)), unchecked((int)(0x9BF264ED)), unchecked((int)(
			0x86F75455)), unchecked((int)(0x4DAB87F0)), unchecked((int)(0xCB3FF55E)), unchecked(
			(int)(0x006326FB)), unchecked((int)(0xF135942E)), unchecked((int)(0x3A69478B)), 
			unchecked((int)(0xBCFD3525)), unchecked((int)(0x77A1E680)), unchecked((int)(0x6AA4D638
			)), unchecked((int)(0xA1F8059D)), unchecked((int)(0x276C7733)), unchecked((int)(
			0xEC30A496)), unchecked((int)(0x191C11EE)), unchecked((int)(0xD240C24B)), unchecked(
			(int)(0x54D4B0E5)), unchecked((int)(0x9F886340)), unchecked((int)(0x828D53F8)), 
			unchecked((int)(0x49D1805D)), unchecked((int)(0xCF45F2F3)), unchecked((int)(0x04192156
			)), unchecked((int)(0xF54F9383)), unchecked((int)(0x3E134026)), unchecked((int)(
			0xB8873288)), unchecked((int)(0x73DBE12D)), unchecked((int)(0x6EDED195)), unchecked(
			(int)(0xA5820230)), unchecked((int)(0x2316709E)), unchecked((int)(0xE84AA33B)), 
			unchecked((int)(0x1ACA1375)), unchecked((int)(0xD196C0D0)), unchecked((int)(0x5702B27E
			)), unchecked((int)(0x9C5E61DB)), unchecked((int)(0x815B5163)), unchecked((int)(
			0x4A0782C6)), unchecked((int)(0xCC93F068)), unchecked((int)(0x07CF23CD)), unchecked(
			(int)(0xF6999118)), unchecked((int)(0x3DC542BD)), unchecked((int)(0xBB513013)), 
			unchecked((int)(0x700DE3B6)), unchecked((int)(0x6D08D30E)), unchecked((int)(0xA65400AB
			)), unchecked((int)(0x20C07205)), unchecked((int)(0xEB9CA1A0)), unchecked((int)(
			0x11E81EB4)), unchecked((int)(0xDAB4CD11)), unchecked((int)(0x5C20BFBF)), unchecked(
			(int)(0x977C6C1A)), unchecked((int)(0x8A795CA2)), unchecked((int)(0x41258F07)), 
			unchecked((int)(0xC7B1FDA9)), unchecked((int)(0x0CED2E0C)), unchecked((int)(0xFDBB9CD9
			)), unchecked((int)(0x36E74F7C)), unchecked((int)(0xB0733DD2)), unchecked((int)(
			0x7B2FEE77)), unchecked((int)(0x662ADECF)), unchecked((int)(0xAD760D6A)), unchecked(
			(int)(0x2BE27FC4)), unchecked((int)(0xE0BEAC61)), unchecked((int)(0x123E1C2F)), 
			unchecked((int)(0xD962CF8A)), unchecked((int)(0x5FF6BD24)), unchecked((int)(0x94AA6E81
			)), unchecked((int)(0x89AF5E39)), unchecked((int)(0x42F38D9C)), unchecked((int)(
			0xC467FF32)), unchecked((int)(0x0F3B2C97)), unchecked((int)(0xFE6D9E42)), unchecked(
			(int)(0x35314DE7)), unchecked((int)(0xB3A53F49)), unchecked((int)(0x78F9ECEC)), 
			unchecked((int)(0x65FCDC54)), unchecked((int)(0xAEA00FF1)), unchecked((int)(0x28347D5F
			)), unchecked((int)(0xE368AEFA)), unchecked((int)(0x16441B82)), unchecked((int)(
			0xDD18C827)), unchecked((int)(0x5B8CBA89)), unchecked((int)(0x90D0692C)), unchecked(
			(int)(0x8DD55994)), unchecked((int)(0x46898A31)), unchecked((int)(0xC01DF89F)), 
			unchecked((int)(0x0B412B3A)), unchecked((int)(0xFA1799EF)), unchecked((int)(0x314B4A4A
			)), unchecked((int)(0xB7DF38E4)), unchecked((int)(0x7C83EB41)), unchecked((int)(
			0x6186DBF9)), unchecked((int)(0xAADA085C)), unchecked((int)(0x2C4E7AF2)), unchecked(
			(int)(0xE712A957)), unchecked((int)(0x15921919)), unchecked((int)(0xDECECABC)), 
			unchecked((int)(0x585AB812)), unchecked((int)(0x93066BB7)), unchecked((int)(0x8E035B0F
			)), unchecked((int)(0x455F88AA)), unchecked((int)(0xC3CBFA04)), unchecked((int)(
			0x089729A1)), unchecked((int)(0xF9C19B74)), unchecked((int)(0x329D48D1)), unchecked(
			(int)(0xB4093A7F)), unchecked((int)(0x7F55E9DA)), unchecked((int)(0x6250D962)), 
			unchecked((int)(0xA90C0AC7)), unchecked((int)(0x2F987869)), unchecked((int)(0xE4C4ABCC
			)), unchecked((int)(0x00000000)), unchecked((int)(0xA6770BB4)), unchecked((int)(
			0x979F1129)), unchecked((int)(0x31E81A9D)), unchecked((int)(0xF44F2413)), unchecked(
			(int)(0x52382FA7)), unchecked((int)(0x63D0353A)), unchecked((int)(0xC5A73E8E)), 
			unchecked((int)(0x33EF4E67)), unchecked((int)(0x959845D3)), unchecked((int)(0xA4705F4E
			)), unchecked((int)(0x020754FA)), unchecked((int)(0xC7A06A74)), unchecked((int)(
			0x61D761C0)), unchecked((int)(0x503F7B5D)), unchecked((int)(0xF64870E9)), unchecked(
			(int)(0x67DE9CCE)), unchecked((int)(0xC1A9977A)), unchecked((int)(0xF0418DE7)), 
			unchecked((int)(0x56368653)), unchecked((int)(0x9391B8DD)), unchecked((int)(0x35E6B369
			)), unchecked((int)(0x040EA9F4)), unchecked((int)(0xA279A240)), unchecked((int)(
			0x5431D2A9)), unchecked((int)(0xF246D91D)), unchecked((int)(0xC3AEC380)), unchecked(
			(int)(0x65D9C834)), unchecked((int)(0xA07EF6BA)), unchecked((int)(0x0609FD0E)), 
			unchecked((int)(0x37E1E793)), unchecked((int)(0x9196EC27)), unchecked((int)(0xCFBD399C
			)), unchecked((int)(0x69CA3228)), unchecked((int)(0x582228B5)), unchecked((int)(
			0xFE552301)), unchecked((int)(0x3BF21D8F)), unchecked((int)(0x9D85163B)), unchecked(
			(int)(0xAC6D0CA6)), unchecked((int)(0x0A1A0712)), unchecked((int)(0xFC5277FB)), 
			unchecked((int)(0x5A257C4F)), unchecked((int)(0x6BCD66D2)), unchecked((int)(0xCDBA6D66
			)), unchecked((int)(0x081D53E8)), unchecked((int)(0xAE6A585C)), unchecked((int)(
			0x9F8242C1)), unchecked((int)(0x39F54975)), unchecked((int)(0xA863A552)), unchecked(
			(int)(0x0E14AEE6)), unchecked((int)(0x3FFCB47B)), unchecked((int)(0x998BBFCF)), 
			unchecked((int)(0x5C2C8141)), unchecked((int)(0xFA5B8AF5)), unchecked((int)(0xCBB39068
			)), unchecked((int)(0x6DC49BDC)), unchecked((int)(0x9B8CEB35)), unchecked((int)(
			0x3DFBE081)), unchecked((int)(0x0C13FA1C)), unchecked((int)(0xAA64F1A8)), unchecked(
			(int)(0x6FC3CF26)), unchecked((int)(0xC9B4C492)), unchecked((int)(0xF85CDE0F)), 
			unchecked((int)(0x5E2BD5BB)), unchecked((int)(0x440B7579)), unchecked((int)(0xE27C7ECD
			)), unchecked((int)(0xD3946450)), unchecked((int)(0x75E36FE4)), unchecked((int)(
			0xB044516A)), unchecked((int)(0x16335ADE)), unchecked((int)(0x27DB4043)), unchecked(
			(int)(0x81AC4BF7)), unchecked((int)(0x77E43B1E)), unchecked((int)(0xD19330AA)), 
			unchecked((int)(0xE07B2A37)), unchecked((int)(0x460C2183)), unchecked((int)(0x83AB1F0D
			)), unchecked((int)(0x25DC14B9)), unchecked((int)(0x14340E24)), unchecked((int)(
			0xB2430590)), unchecked((int)(0x23D5E9B7)), unchecked((int)(0x85A2E203)), unchecked(
			(int)(0xB44AF89E)), unchecked((int)(0x123DF32A)), unchecked((int)(0xD79ACDA4)), 
			unchecked((int)(0x71EDC610)), unchecked((int)(0x4005DC8D)), unchecked((int)(0xE672D739
			)), unchecked((int)(0x103AA7D0)), unchecked((int)(0xB64DAC64)), unchecked((int)(
			0x87A5B6F9)), unchecked((int)(0x21D2BD4D)), unchecked((int)(0xE47583C3)), unchecked(
			(int)(0x42028877)), unchecked((int)(0x73EA92EA)), unchecked((int)(0xD59D995E)), 
			unchecked((int)(0x8BB64CE5)), unchecked((int)(0x2DC14751)), unchecked((int)(0x1C295DCC
			)), unchecked((int)(0xBA5E5678)), unchecked((int)(0x7FF968F6)), unchecked((int)(
			0xD98E6342)), unchecked((int)(0xE86679DF)), unchecked((int)(0x4E11726B)), unchecked(
			(int)(0xB8590282)), unchecked((int)(0x1E2E0936)), unchecked((int)(0x2FC613AB)), 
			unchecked((int)(0x89B1181F)), unchecked((int)(0x4C162691)), unchecked((int)(0xEA612D25
			)), unchecked((int)(0xDB8937B8)), unchecked((int)(0x7DFE3C0C)), unchecked((int)(
			0xEC68D02B)), unchecked((int)(0x4A1FDB9F)), unchecked((int)(0x7BF7C102)), unchecked(
			(int)(0xDD80CAB6)), unchecked((int)(0x1827F438)), unchecked((int)(0xBE50FF8C)), 
			unchecked((int)(0x8FB8E511)), unchecked((int)(0x29CFEEA5)), unchecked((int)(0xDF879E4C
			)), unchecked((int)(0x79F095F8)), unchecked((int)(0x48188F65)), unchecked((int)(
			0xEE6F84D1)), unchecked((int)(0x2BC8BA5F)), unchecked((int)(0x8DBFB1EB)), unchecked(
			(int)(0xBC57AB76)), unchecked((int)(0x1A20A0C2)), unchecked((int)(0x8816EAF2)), 
			unchecked((int)(0x2E61E146)), unchecked((int)(0x1F89FBDB)), unchecked((int)(0xB9FEF06F
			)), unchecked((int)(0x7C59CEE1)), unchecked((int)(0xDA2EC555)), unchecked((int)(
			0xEBC6DFC8)), unchecked((int)(0x4DB1D47C)), unchecked((int)(0xBBF9A495)), unchecked(
			(int)(0x1D8EAF21)), unchecked((int)(0x2C66B5BC)), unchecked((int)(0x8A11BE08)), 
			unchecked((int)(0x4FB68086)), unchecked((int)(0xE9C18B32)), unchecked((int)(0xD82991AF
			)), unchecked((int)(0x7E5E9A1B)), unchecked((int)(0xEFC8763C)), unchecked((int)(
			0x49BF7D88)), unchecked((int)(0x78576715)), unchecked((int)(0xDE206CA1)), unchecked(
			(int)(0x1B87522F)), unchecked((int)(0xBDF0599B)), unchecked((int)(0x8C184306)), 
			unchecked((int)(0x2A6F48B2)), unchecked((int)(0xDC27385B)), unchecked((int)(0x7A5033EF
			)), unchecked((int)(0x4BB82972)), unchecked((int)(0xEDCF22C6)), unchecked((int)(
			0x28681C48)), unchecked((int)(0x8E1F17FC)), unchecked((int)(0xBFF70D61)), unchecked(
			(int)(0x198006D5)), unchecked((int)(0x47ABD36E)), unchecked((int)(0xE1DCD8DA)), 
			unchecked((int)(0xD034C247)), unchecked((int)(0x7643C9F3)), unchecked((int)(0xB3E4F77D
			)), unchecked((int)(0x1593FCC9)), unchecked((int)(0x247BE654)), unchecked((int)(
			0x820CEDE0)), unchecked((int)(0x74449D09)), unchecked((int)(0xD23396BD)), unchecked(
			(int)(0xE3DB8C20)), unchecked((int)(0x45AC8794)), unchecked((int)(0x800BB91A)), 
			unchecked((int)(0x267CB2AE)), unchecked((int)(0x1794A833)), unchecked((int)(0xB1E3A387
			)), unchecked((int)(0x20754FA0)), unchecked((int)(0x86024414)), unchecked((int)(
			0xB7EA5E89)), unchecked((int)(0x119D553D)), unchecked((int)(0xD43A6BB3)), unchecked(
			(int)(0x724D6007)), unchecked((int)(0x43A57A9A)), unchecked((int)(0xE5D2712E)), 
			unchecked((int)(0x139A01C7)), unchecked((int)(0xB5ED0A73)), unchecked((int)(0x840510EE
			)), unchecked((int)(0x22721B5A)), unchecked((int)(0xE7D525D4)), unchecked((int)(
			0x41A22E60)), unchecked((int)(0x704A34FD)), unchecked((int)(0xD63D3F49)), unchecked(
			(int)(0xCC1D9F8B)), unchecked((int)(0x6A6A943F)), unchecked((int)(0x5B828EA2)), 
			unchecked((int)(0xFDF58516)), unchecked((int)(0x3852BB98)), unchecked((int)(0x9E25B02C
			)), unchecked((int)(0xAFCDAAB1)), unchecked((int)(0x09BAA105)), unchecked((int)(
			0xFFF2D1EC)), unchecked((int)(0x5985DA58)), unchecked((int)(0x686DC0C5)), unchecked(
			(int)(0xCE1ACB71)), unchecked((int)(0x0BBDF5FF)), unchecked((int)(0xADCAFE4B)), 
			unchecked((int)(0x9C22E4D6)), unchecked((int)(0x3A55EF62)), unchecked((int)(0xABC30345
			)), unchecked((int)(0x0DB408F1)), unchecked((int)(0x3C5C126C)), unchecked((int)(
			0x9A2B19D8)), unchecked((int)(0x5F8C2756)), unchecked((int)(0xF9FB2CE2)), unchecked(
			(int)(0xC813367F)), unchecked((int)(0x6E643DCB)), unchecked((int)(0x982C4D22)), 
			unchecked((int)(0x3E5B4696)), unchecked((int)(0x0FB35C0B)), unchecked((int)(0xA9C457BF
			)), unchecked((int)(0x6C636931)), unchecked((int)(0xCA146285)), unchecked((int)(
			0xFBFC7818)), unchecked((int)(0x5D8B73AC)), unchecked((int)(0x03A0A617)), unchecked(
			(int)(0xA5D7ADA3)), unchecked((int)(0x943FB73E)), unchecked((int)(0x3248BC8A)), 
			unchecked((int)(0xF7EF8204)), unchecked((int)(0x519889B0)), unchecked((int)(0x6070932D
			)), unchecked((int)(0xC6079899)), unchecked((int)(0x304FE870)), unchecked((int)(
			0x9638E3C4)), unchecked((int)(0xA7D0F959)), unchecked((int)(0x01A7F2ED)), unchecked(
			(int)(0xC400CC63)), unchecked((int)(0x6277C7D7)), unchecked((int)(0x539FDD4A)), 
			unchecked((int)(0xF5E8D6FE)), unchecked((int)(0x647E3AD9)), unchecked((int)(0xC209316D
			)), unchecked((int)(0xF3E12BF0)), unchecked((int)(0x55962044)), unchecked((int)(
			0x90311ECA)), unchecked((int)(0x3646157E)), unchecked((int)(0x07AE0FE3)), unchecked(
			(int)(0xA1D90457)), unchecked((int)(0x579174BE)), unchecked((int)(0xF1E67F0A)), 
			unchecked((int)(0xC00E6597)), unchecked((int)(0x66796E23)), unchecked((int)(0xA3DE50AD
			)), unchecked((int)(0x05A95B19)), unchecked((int)(0x34414184)), unchecked((int)(
			0x92364A30)), unchecked((int)(0x00000000)), unchecked((int)(0xCCAA009E)), unchecked(
			(int)(0x4225077D)), unchecked((int)(0x8E8F07E3)), unchecked((int)(0x844A0EFA)), 
			unchecked((int)(0x48E00E64)), unchecked((int)(0xC66F0987)), unchecked((int)(0x0AC50919
			)), unchecked((int)(0xD3E51BB5)), unchecked((int)(0x1F4F1B2B)), unchecked((int)(
			0x91C01CC8)), unchecked((int)(0x5D6A1C56)), unchecked((int)(0x57AF154F)), unchecked(
			(int)(0x9B0515D1)), unchecked((int)(0x158A1232)), unchecked((int)(0xD92012AC)), 
			unchecked((int)(0x7CBB312B)), unchecked((int)(0xB01131B5)), unchecked((int)(0x3E9E3656
			)), unchecked((int)(0xF23436C8)), unchecked((int)(0xF8F13FD1)), unchecked((int)(
			0x345B3F4F)), unchecked((int)(0xBAD438AC)), unchecked((int)(0x767E3832)), unchecked(
			(int)(0xAF5E2A9E)), unchecked((int)(0x63F42A00)), unchecked((int)(0xED7B2DE3)), 
			unchecked((int)(0x21D12D7D)), unchecked((int)(0x2B142464)), unchecked((int)(0xE7BE24FA
			)), unchecked((int)(0x69312319)), unchecked((int)(0xA59B2387)), unchecked((int)(
			0xF9766256)), unchecked((int)(0x35DC62C8)), unchecked((int)(0xBB53652B)), unchecked(
			(int)(0x77F965B5)), unchecked((int)(0x7D3C6CAC)), unchecked((int)(0xB1966C32)), 
			unchecked((int)(0x3F196BD1)), unchecked((int)(0xF3B36B4F)), unchecked((int)(0x2A9379E3
			)), unchecked((int)(0xE639797D)), unchecked((int)(0x68B67E9E)), unchecked((int)(
			0xA41C7E00)), unchecked((int)(0xAED97719)), unchecked((int)(0x62737787)), unchecked(
			(int)(0xECFC7064)), unchecked((int)(0x205670FA)), unchecked((int)(0x85CD537D)), 
			unchecked((int)(0x496753E3)), unchecked((int)(0xC7E85400)), unchecked((int)(0x0B42549E
			)), unchecked((int)(0x01875D87)), unchecked((int)(0xCD2D5D19)), unchecked((int)(
			0x43A25AFA)), unchecked((int)(0x8F085A64)), unchecked((int)(0x562848C8)), unchecked(
			(int)(0x9A824856)), unchecked((int)(0x140D4FB5)), unchecked((int)(0xD8A74F2B)), 
			unchecked((int)(0xD2624632)), unchecked((int)(0x1EC846AC)), unchecked((int)(0x9047414F
			)), unchecked((int)(0x5CED41D1)), unchecked((int)(0x299DC2ED)), unchecked((int)(
			0xE537C273)), unchecked((int)(0x6BB8C590)), unchecked((int)(0xA712C50E)), unchecked(
			(int)(0xADD7CC17)), unchecked((int)(0x617DCC89)), unchecked((int)(0xEFF2CB6A)), 
			unchecked((int)(0x2358CBF4)), unchecked((int)(0xFA78D958)), unchecked((int)(0x36D2D9C6
			)), unchecked((int)(0xB85DDE25)), unchecked((int)(0x74F7DEBB)), unchecked((int)(
			0x7E32D7A2)), unchecked((int)(0xB298D73C)), unchecked((int)(0x3C17D0DF)), unchecked(
			(int)(0xF0BDD041)), unchecked((int)(0x5526F3C6)), unchecked((int)(0x998CF358)), 
			unchecked((int)(0x1703F4BB)), unchecked((int)(0xDBA9F425)), unchecked((int)(0xD16CFD3C
			)), unchecked((int)(0x1DC6FDA2)), unchecked((int)(0x9349FA41)), unchecked((int)(
			0x5FE3FADF)), unchecked((int)(0x86C3E873)), unchecked((int)(0x4A69E8ED)), unchecked(
			(int)(0xC4E6EF0E)), unchecked((int)(0x084CEF90)), unchecked((int)(0x0289E689)), 
			unchecked((int)(0xCE23E617)), unchecked((int)(0x40ACE1F4)), unchecked((int)(0x8C06E16A
			)), unchecked((int)(0xD0EBA0BB)), unchecked((int)(0x1C41A025)), unchecked((int)(
			0x92CEA7C6)), unchecked((int)(0x5E64A758)), unchecked((int)(0x54A1AE41)), unchecked(
			(int)(0x980BAEDF)), unchecked((int)(0x1684A93C)), unchecked((int)(0xDA2EA9A2)), 
			unchecked((int)(0x030EBB0E)), unchecked((int)(0xCFA4BB90)), unchecked((int)(0x412BBC73
			)), unchecked((int)(0x8D81BCED)), unchecked((int)(0x8744B5F4)), unchecked((int)(
			0x4BEEB56A)), unchecked((int)(0xC561B289)), unchecked((int)(0x09CBB217)), unchecked(
			(int)(0xAC509190)), unchecked((int)(0x60FA910E)), unchecked((int)(0xEE7596ED)), 
			unchecked((int)(0x22DF9673)), unchecked((int)(0x281A9F6A)), unchecked((int)(0xE4B09FF4
			)), unchecked((int)(0x6A3F9817)), unchecked((int)(0xA6959889)), unchecked((int)(
			0x7FB58A25)), unchecked((int)(0xB31F8ABB)), unchecked((int)(0x3D908D58)), unchecked(
			(int)(0xF13A8DC6)), unchecked((int)(0xFBFF84DF)), unchecked((int)(0x37558441)), 
			unchecked((int)(0xB9DA83A2)), unchecked((int)(0x7570833C)), unchecked((int)(0x533B85DA
			)), unchecked((int)(0x9F918544)), unchecked((int)(0x111E82A7)), unchecked((int)(
			0xDDB48239)), unchecked((int)(0xD7718B20)), unchecked((int)(0x1BDB8BBE)), unchecked(
			(int)(0x95548C5D)), unchecked((int)(0x59FE8CC3)), unchecked((int)(0x80DE9E6F)), 
			unchecked((int)(0x4C749EF1)), unchecked((int)(0xC2FB9912)), unchecked((int)(0x0E51998C
			)), unchecked((int)(0x04949095)), unchecked((int)(0xC83E900B)), unchecked((int)(
			0x46B197E8)), unchecked((int)(0x8A1B9776)), unchecked((int)(0x2F80B4F1)), unchecked(
			(int)(0xE32AB46F)), unchecked((int)(0x6DA5B38C)), unchecked((int)(0xA10FB312)), 
			unchecked((int)(0xABCABA0B)), unchecked((int)(0x6760BA95)), unchecked((int)(0xE9EFBD76
			)), unchecked((int)(0x2545BDE8)), unchecked((int)(0xFC65AF44)), unchecked((int)(
			0x30CFAFDA)), unchecked((int)(0xBE40A839)), unchecked((int)(0x72EAA8A7)), unchecked(
			(int)(0x782FA1BE)), unchecked((int)(0xB485A120)), unchecked((int)(0x3A0AA6C3)), 
			unchecked((int)(0xF6A0A65D)), unchecked((int)(0xAA4DE78C)), unchecked((int)(0x66E7E712
			)), unchecked((int)(0xE868E0F1)), unchecked((int)(0x24C2E06F)), unchecked((int)(
			0x2E07E976)), unchecked((int)(0xE2ADE9E8)), unchecked((int)(0x6C22EE0B)), unchecked(
			(int)(0xA088EE95)), unchecked((int)(0x79A8FC39)), unchecked((int)(0xB502FCA7)), 
			unchecked((int)(0x3B8DFB44)), unchecked((int)(0xF727FBDA)), unchecked((int)(0xFDE2F2C3
			)), unchecked((int)(0x3148F25D)), unchecked((int)(0xBFC7F5BE)), unchecked((int)(
			0x736DF520)), unchecked((int)(0xD6F6D6A7)), unchecked((int)(0x1A5CD639)), unchecked(
			(int)(0x94D3D1DA)), unchecked((int)(0x5879D144)), unchecked((int)(0x52BCD85D)), 
			unchecked((int)(0x9E16D8C3)), unchecked((int)(0x1099DF20)), unchecked((int)(0xDC33DFBE
			)), unchecked((int)(0x0513CD12)), unchecked((int)(0xC9B9CD8C)), unchecked((int)(
			0x4736CA6F)), unchecked((int)(0x8B9CCAF1)), unchecked((int)(0x8159C3E8)), unchecked(
			(int)(0x4DF3C376)), unchecked((int)(0xC37CC495)), unchecked((int)(0x0FD6C40B)), 
			unchecked((int)(0x7AA64737)), unchecked((int)(0xB60C47A9)), unchecked((int)(0x3883404A
			)), unchecked((int)(0xF42940D4)), unchecked((int)(0xFEEC49CD)), unchecked((int)(
			0x32464953)), unchecked((int)(0xBCC94EB0)), unchecked((int)(0x70634E2E)), unchecked(
			(int)(0xA9435C82)), unchecked((int)(0x65E95C1C)), unchecked((int)(0xEB665BFF)), 
			unchecked((int)(0x27CC5B61)), unchecked((int)(0x2D095278)), unchecked((int)(0xE1A352E6
			)), unchecked((int)(0x6F2C5505)), unchecked((int)(0xA386559B)), unchecked((int)(
			0x061D761C)), unchecked((int)(0xCAB77682)), unchecked((int)(0x44387161)), unchecked(
			(int)(0x889271FF)), unchecked((int)(0x825778E6)), unchecked((int)(0x4EFD7878)), 
			unchecked((int)(0xC0727F9B)), unchecked((int)(0x0CD87F05)), unchecked((int)(0xD5F86DA9
			)), unchecked((int)(0x19526D37)), unchecked((int)(0x97DD6AD4)), unchecked((int)(
			0x5B776A4A)), unchecked((int)(0x51B26353)), unchecked((int)(0x9D1863CD)), unchecked(
			(int)(0x1397642E)), unchecked((int)(0xDF3D64B0)), unchecked((int)(0x83D02561)), 
			unchecked((int)(0x4F7A25FF)), unchecked((int)(0xC1F5221C)), unchecked((int)(0x0D5F2282
			)), unchecked((int)(0x079A2B9B)), unchecked((int)(0xCB302B05)), unchecked((int)(
			0x45BF2CE6)), unchecked((int)(0x89152C78)), unchecked((int)(0x50353ED4)), unchecked(
			(int)(0x9C9F3E4A)), unchecked((int)(0x121039A9)), unchecked((int)(0xDEBA3937)), 
			unchecked((int)(0xD47F302E)), unchecked((int)(0x18D530B0)), unchecked((int)(0x965A3753
			)), unchecked((int)(0x5AF037CD)), unchecked((int)(0xFF6B144A)), unchecked((int)(
			0x33C114D4)), unchecked((int)(0xBD4E1337)), unchecked((int)(0x71E413A9)), unchecked(
			(int)(0x7B211AB0)), unchecked((int)(0xB78B1A2E)), unchecked((int)(0x39041DCD)), 
			unchecked((int)(0xF5AE1D53)), unchecked((int)(0x2C8E0FFF)), unchecked((int)(0xE0240F61
			)), unchecked((int)(0x6EAB0882)), unchecked((int)(0xA201081C)), unchecked((int)(
			0xA8C40105)), unchecked((int)(0x646E019B)), unchecked((int)(0xEAE10678)), unchecked(
			(int)(0x264B06E6)) };
		/*
		* CRC-32 lookup tables generated by the polynomial 0xEDB88320.
		* See also TestPureJavaCrc32.Table.
		*/
		/* T8_0 */
		/* T8_1 */
		/* T8_2 */
		/* T8_3 */
		/* T8_4 */
		/* T8_5 */
		/* T8_6 */
		/* T8_7 */
	}
}
