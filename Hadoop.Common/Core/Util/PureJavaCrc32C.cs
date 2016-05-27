using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>
	/// A pure-java implementation of the CRC32 checksum that uses
	/// the CRC32-C polynomial, the same polynomial used by iSCSI
	/// and implemented on many Intel chipsets supporting SSE4.2.
	/// </summary>
	public class PureJavaCrc32C : Checksum
	{
		/// <summary>the current CRC value, bit-flipped</summary>
		private int crc;

		/// <summary>Create a new PureJavaCrc32 object.</summary>
		public PureJavaCrc32C()
		{
			Reset();
		}

		public virtual long GetValue()
		{
			long ret = crc;
			return (~ret) & unchecked((long)(0xffffffffL));
		}

		public virtual void Reset()
		{
			crc = unchecked((int)(0xffffffff));
		}

		public virtual void Update(byte[] b, int off, int len)
		{
			int localCrc = crc;
			while (len > 7)
			{
				int c0 = (b[off + 0] ^ localCrc) & unchecked((int)(0xff));
				int c1 = (b[off + 1] ^ (localCrc = (int)(((uint)localCrc) >> 8))) & unchecked((int
					)(0xff));
				int c2 = (b[off + 2] ^ (localCrc = (int)(((uint)localCrc) >> 8))) & unchecked((int
					)(0xff));
				int c3 = (b[off + 3] ^ (localCrc = (int)(((uint)localCrc) >> 8))) & unchecked((int
					)(0xff));
				localCrc = (T[T8_7_start + c0] ^ T[T8_6_start + c1]) ^ (T[T8_5_start + c2] ^ T[T8_4_start
					 + c3]);
				int c4 = b[off + 4] & unchecked((int)(0xff));
				int c5 = b[off + 5] & unchecked((int)(0xff));
				int c6 = b[off + 6] & unchecked((int)(0xff));
				int c7 = b[off + 7] & unchecked((int)(0xff));
				localCrc ^= (T[T8_3_start + c4] ^ T[T8_2_start + c5]) ^ (T[T8_1_start + c6] ^ T[T8_0_start
					 + c7]);
				off += 8;
				len -= 8;
			}
			switch (len)
			{
				case 7:
				{
					/* loop unroll - duff's device style */
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[T8_0_start + ((localCrc ^ b[off++])
						 & unchecked((int)(0xff)))];
					goto case 6;
				}

				case 6:
				{
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[T8_0_start + ((localCrc ^ b[off++])
						 & unchecked((int)(0xff)))];
					goto case 5;
				}

				case 5:
				{
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[T8_0_start + ((localCrc ^ b[off++])
						 & unchecked((int)(0xff)))];
					goto case 4;
				}

				case 4:
				{
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[T8_0_start + ((localCrc ^ b[off++])
						 & unchecked((int)(0xff)))];
					goto case 3;
				}

				case 3:
				{
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[T8_0_start + ((localCrc ^ b[off++])
						 & unchecked((int)(0xff)))];
					goto case 2;
				}

				case 2:
				{
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[T8_0_start + ((localCrc ^ b[off++])
						 & unchecked((int)(0xff)))];
					goto case 1;
				}

				case 1:
				{
					localCrc = ((int)(((uint)localCrc) >> 8)) ^ T[T8_0_start + ((localCrc ^ b[off++])
						 & unchecked((int)(0xff)))];
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
			crc = ((int)(((uint)crc) >> 8)) ^ T[T8_0_start + ((crc ^ b) & unchecked((int)(0xff
				)))];
		}

		private const int T8_0_start = 0 * 256;

		private const int T8_1_start = 1 * 256;

		private const int T8_2_start = 2 * 256;

		private const int T8_3_start = 3 * 256;

		private const int T8_4_start = 4 * 256;

		private const int T8_5_start = 5 * 256;

		private const int T8_6_start = 6 * 256;

		private const int T8_7_start = 7 * 256;

		private static readonly int[] T = new int[] { unchecked((int)(0x00000000)), unchecked(
			(int)(0xF26B8303)), unchecked((int)(0xE13B70F7)), unchecked((int)(0x1350F3F4)), 
			unchecked((int)(0xC79A971F)), unchecked((int)(0x35F1141C)), unchecked((int)(0x26A1E7E8
			)), unchecked((int)(0xD4CA64EB)), unchecked((int)(0x8AD958CF)), unchecked((int)(
			0x78B2DBCC)), unchecked((int)(0x6BE22838)), unchecked((int)(0x9989AB3B)), unchecked(
			(int)(0x4D43CFD0)), unchecked((int)(0xBF284CD3)), unchecked((int)(0xAC78BF27)), 
			unchecked((int)(0x5E133C24)), unchecked((int)(0x105EC76F)), unchecked((int)(0xE235446C
			)), unchecked((int)(0xF165B798)), unchecked((int)(0x030E349B)), unchecked((int)(
			0xD7C45070)), unchecked((int)(0x25AFD373)), unchecked((int)(0x36FF2087)), unchecked(
			(int)(0xC494A384)), unchecked((int)(0x9A879FA0)), unchecked((int)(0x68EC1CA3)), 
			unchecked((int)(0x7BBCEF57)), unchecked((int)(0x89D76C54)), unchecked((int)(0x5D1D08BF
			)), unchecked((int)(0xAF768BBC)), unchecked((int)(0xBC267848)), unchecked((int)(
			0x4E4DFB4B)), unchecked((int)(0x20BD8EDE)), unchecked((int)(0xD2D60DDD)), unchecked(
			(int)(0xC186FE29)), unchecked((int)(0x33ED7D2A)), unchecked((int)(0xE72719C1)), 
			unchecked((int)(0x154C9AC2)), unchecked((int)(0x061C6936)), unchecked((int)(0xF477EA35
			)), unchecked((int)(0xAA64D611)), unchecked((int)(0x580F5512)), unchecked((int)(
			0x4B5FA6E6)), unchecked((int)(0xB93425E5)), unchecked((int)(0x6DFE410E)), unchecked(
			(int)(0x9F95C20D)), unchecked((int)(0x8CC531F9)), unchecked((int)(0x7EAEB2FA)), 
			unchecked((int)(0x30E349B1)), unchecked((int)(0xC288CAB2)), unchecked((int)(0xD1D83946
			)), unchecked((int)(0x23B3BA45)), unchecked((int)(0xF779DEAE)), unchecked((int)(
			0x05125DAD)), unchecked((int)(0x1642AE59)), unchecked((int)(0xE4292D5A)), unchecked(
			(int)(0xBA3A117E)), unchecked((int)(0x4851927D)), unchecked((int)(0x5B016189)), 
			unchecked((int)(0xA96AE28A)), unchecked((int)(0x7DA08661)), unchecked((int)(0x8FCB0562
			)), unchecked((int)(0x9C9BF696)), unchecked((int)(0x6EF07595)), unchecked((int)(
			0x417B1DBC)), unchecked((int)(0xB3109EBF)), unchecked((int)(0xA0406D4B)), unchecked(
			(int)(0x522BEE48)), unchecked((int)(0x86E18AA3)), unchecked((int)(0x748A09A0)), 
			unchecked((int)(0x67DAFA54)), unchecked((int)(0x95B17957)), unchecked((int)(0xCBA24573
			)), unchecked((int)(0x39C9C670)), unchecked((int)(0x2A993584)), unchecked((int)(
			0xD8F2B687)), unchecked((int)(0x0C38D26C)), unchecked((int)(0xFE53516F)), unchecked(
			(int)(0xED03A29B)), unchecked((int)(0x1F682198)), unchecked((int)(0x5125DAD3)), 
			unchecked((int)(0xA34E59D0)), unchecked((int)(0xB01EAA24)), unchecked((int)(0x42752927
			)), unchecked((int)(0x96BF4DCC)), unchecked((int)(0x64D4CECF)), unchecked((int)(
			0x77843D3B)), unchecked((int)(0x85EFBE38)), unchecked((int)(0xDBFC821C)), unchecked(
			(int)(0x2997011F)), unchecked((int)(0x3AC7F2EB)), unchecked((int)(0xC8AC71E8)), 
			unchecked((int)(0x1C661503)), unchecked((int)(0xEE0D9600)), unchecked((int)(0xFD5D65F4
			)), unchecked((int)(0x0F36E6F7)), unchecked((int)(0x61C69362)), unchecked((int)(
			0x93AD1061)), unchecked((int)(0x80FDE395)), unchecked((int)(0x72966096)), unchecked(
			(int)(0xA65C047D)), unchecked((int)(0x5437877E)), unchecked((int)(0x4767748A)), 
			unchecked((int)(0xB50CF789)), unchecked((int)(0xEB1FCBAD)), unchecked((int)(0x197448AE
			)), unchecked((int)(0x0A24BB5A)), unchecked((int)(0xF84F3859)), unchecked((int)(
			0x2C855CB2)), unchecked((int)(0xDEEEDFB1)), unchecked((int)(0xCDBE2C45)), unchecked(
			(int)(0x3FD5AF46)), unchecked((int)(0x7198540D)), unchecked((int)(0x83F3D70E)), 
			unchecked((int)(0x90A324FA)), unchecked((int)(0x62C8A7F9)), unchecked((int)(0xB602C312
			)), unchecked((int)(0x44694011)), unchecked((int)(0x5739B3E5)), unchecked((int)(
			0xA55230E6)), unchecked((int)(0xFB410CC2)), unchecked((int)(0x092A8FC1)), unchecked(
			(int)(0x1A7A7C35)), unchecked((int)(0xE811FF36)), unchecked((int)(0x3CDB9BDD)), 
			unchecked((int)(0xCEB018DE)), unchecked((int)(0xDDE0EB2A)), unchecked((int)(0x2F8B6829
			)), unchecked((int)(0x82F63B78)), unchecked((int)(0x709DB87B)), unchecked((int)(
			0x63CD4B8F)), unchecked((int)(0x91A6C88C)), unchecked((int)(0x456CAC67)), unchecked(
			(int)(0xB7072F64)), unchecked((int)(0xA457DC90)), unchecked((int)(0x563C5F93)), 
			unchecked((int)(0x082F63B7)), unchecked((int)(0xFA44E0B4)), unchecked((int)(0xE9141340
			)), unchecked((int)(0x1B7F9043)), unchecked((int)(0xCFB5F4A8)), unchecked((int)(
			0x3DDE77AB)), unchecked((int)(0x2E8E845F)), unchecked((int)(0xDCE5075C)), unchecked(
			(int)(0x92A8FC17)), unchecked((int)(0x60C37F14)), unchecked((int)(0x73938CE0)), 
			unchecked((int)(0x81F80FE3)), unchecked((int)(0x55326B08)), unchecked((int)(0xA759E80B
			)), unchecked((int)(0xB4091BFF)), unchecked((int)(0x466298FC)), unchecked((int)(
			0x1871A4D8)), unchecked((int)(0xEA1A27DB)), unchecked((int)(0xF94AD42F)), unchecked(
			(int)(0x0B21572C)), unchecked((int)(0xDFEB33C7)), unchecked((int)(0x2D80B0C4)), 
			unchecked((int)(0x3ED04330)), unchecked((int)(0xCCBBC033)), unchecked((int)(0xA24BB5A6
			)), unchecked((int)(0x502036A5)), unchecked((int)(0x4370C551)), unchecked((int)(
			0xB11B4652)), unchecked((int)(0x65D122B9)), unchecked((int)(0x97BAA1BA)), unchecked(
			(int)(0x84EA524E)), unchecked((int)(0x7681D14D)), unchecked((int)(0x2892ED69)), 
			unchecked((int)(0xDAF96E6A)), unchecked((int)(0xC9A99D9E)), unchecked((int)(0x3BC21E9D
			)), unchecked((int)(0xEF087A76)), unchecked((int)(0x1D63F975)), unchecked((int)(
			0x0E330A81)), unchecked((int)(0xFC588982)), unchecked((int)(0xB21572C9)), unchecked(
			(int)(0x407EF1CA)), unchecked((int)(0x532E023E)), unchecked((int)(0xA145813D)), 
			unchecked((int)(0x758FE5D6)), unchecked((int)(0x87E466D5)), unchecked((int)(0x94B49521
			)), unchecked((int)(0x66DF1622)), unchecked((int)(0x38CC2A06)), unchecked((int)(
			0xCAA7A905)), unchecked((int)(0xD9F75AF1)), unchecked((int)(0x2B9CD9F2)), unchecked(
			(int)(0xFF56BD19)), unchecked((int)(0x0D3D3E1A)), unchecked((int)(0x1E6DCDEE)), 
			unchecked((int)(0xEC064EED)), unchecked((int)(0xC38D26C4)), unchecked((int)(0x31E6A5C7
			)), unchecked((int)(0x22B65633)), unchecked((int)(0xD0DDD530)), unchecked((int)(
			0x0417B1DB)), unchecked((int)(0xF67C32D8)), unchecked((int)(0xE52CC12C)), unchecked(
			(int)(0x1747422F)), unchecked((int)(0x49547E0B)), unchecked((int)(0xBB3FFD08)), 
			unchecked((int)(0xA86F0EFC)), unchecked((int)(0x5A048DFF)), unchecked((int)(0x8ECEE914
			)), unchecked((int)(0x7CA56A17)), unchecked((int)(0x6FF599E3)), unchecked((int)(
			0x9D9E1AE0)), unchecked((int)(0xD3D3E1AB)), unchecked((int)(0x21B862A8)), unchecked(
			(int)(0x32E8915C)), unchecked((int)(0xC083125F)), unchecked((int)(0x144976B4)), 
			unchecked((int)(0xE622F5B7)), unchecked((int)(0xF5720643)), unchecked((int)(0x07198540
			)), unchecked((int)(0x590AB964)), unchecked((int)(0xAB613A67)), unchecked((int)(
			0xB831C993)), unchecked((int)(0x4A5A4A90)), unchecked((int)(0x9E902E7B)), unchecked(
			(int)(0x6CFBAD78)), unchecked((int)(0x7FAB5E8C)), unchecked((int)(0x8DC0DD8F)), 
			unchecked((int)(0xE330A81A)), unchecked((int)(0x115B2B19)), unchecked((int)(0x020BD8ED
			)), unchecked((int)(0xF0605BEE)), unchecked((int)(0x24AA3F05)), unchecked((int)(
			0xD6C1BC06)), unchecked((int)(0xC5914FF2)), unchecked((int)(0x37FACCF1)), unchecked(
			(int)(0x69E9F0D5)), unchecked((int)(0x9B8273D6)), unchecked((int)(0x88D28022)), 
			unchecked((int)(0x7AB90321)), unchecked((int)(0xAE7367CA)), unchecked((int)(0x5C18E4C9
			)), unchecked((int)(0x4F48173D)), unchecked((int)(0xBD23943E)), unchecked((int)(
			0xF36E6F75)), unchecked((int)(0x0105EC76)), unchecked((int)(0x12551F82)), unchecked(
			(int)(0xE03E9C81)), unchecked((int)(0x34F4F86A)), unchecked((int)(0xC69F7B69)), 
			unchecked((int)(0xD5CF889D)), unchecked((int)(0x27A40B9E)), unchecked((int)(0x79B737BA
			)), unchecked((int)(0x8BDCB4B9)), unchecked((int)(0x988C474D)), unchecked((int)(
			0x6AE7C44E)), unchecked((int)(0xBE2DA0A5)), unchecked((int)(0x4C4623A6)), unchecked(
			(int)(0x5F16D052)), unchecked((int)(0xAD7D5351)), unchecked((int)(0x00000000)), 
			unchecked((int)(0x13A29877)), unchecked((int)(0x274530EE)), unchecked((int)(0x34E7A899
			)), unchecked((int)(0x4E8A61DC)), unchecked((int)(0x5D28F9AB)), unchecked((int)(
			0x69CF5132)), unchecked((int)(0x7A6DC945)), unchecked((int)(0x9D14C3B8)), unchecked(
			(int)(0x8EB65BCF)), unchecked((int)(0xBA51F356)), unchecked((int)(0xA9F36B21)), 
			unchecked((int)(0xD39EA264)), unchecked((int)(0xC03C3A13)), unchecked((int)(0xF4DB928A
			)), unchecked((int)(0xE7790AFD)), unchecked((int)(0x3FC5F181)), unchecked((int)(
			0x2C6769F6)), unchecked((int)(0x1880C16F)), unchecked((int)(0x0B225918)), unchecked(
			(int)(0x714F905D)), unchecked((int)(0x62ED082A)), unchecked((int)(0x560AA0B3)), 
			unchecked((int)(0x45A838C4)), unchecked((int)(0xA2D13239)), unchecked((int)(0xB173AA4E
			)), unchecked((int)(0x859402D7)), unchecked((int)(0x96369AA0)), unchecked((int)(
			0xEC5B53E5)), unchecked((int)(0xFFF9CB92)), unchecked((int)(0xCB1E630B)), unchecked(
			(int)(0xD8BCFB7C)), unchecked((int)(0x7F8BE302)), unchecked((int)(0x6C297B75)), 
			unchecked((int)(0x58CED3EC)), unchecked((int)(0x4B6C4B9B)), unchecked((int)(0x310182DE
			)), unchecked((int)(0x22A31AA9)), unchecked((int)(0x1644B230)), unchecked((int)(
			0x05E62A47)), unchecked((int)(0xE29F20BA)), unchecked((int)(0xF13DB8CD)), unchecked(
			(int)(0xC5DA1054)), unchecked((int)(0xD6788823)), unchecked((int)(0xAC154166)), 
			unchecked((int)(0xBFB7D911)), unchecked((int)(0x8B507188)), unchecked((int)(0x98F2E9FF
			)), unchecked((int)(0x404E1283)), unchecked((int)(0x53EC8AF4)), unchecked((int)(
			0x670B226D)), unchecked((int)(0x74A9BA1A)), unchecked((int)(0x0EC4735F)), unchecked(
			(int)(0x1D66EB28)), unchecked((int)(0x298143B1)), unchecked((int)(0x3A23DBC6)), 
			unchecked((int)(0xDD5AD13B)), unchecked((int)(0xCEF8494C)), unchecked((int)(0xFA1FE1D5
			)), unchecked((int)(0xE9BD79A2)), unchecked((int)(0x93D0B0E7)), unchecked((int)(
			0x80722890)), unchecked((int)(0xB4958009)), unchecked((int)(0xA737187E)), unchecked(
			(int)(0xFF17C604)), unchecked((int)(0xECB55E73)), unchecked((int)(0xD852F6EA)), 
			unchecked((int)(0xCBF06E9D)), unchecked((int)(0xB19DA7D8)), unchecked((int)(0xA23F3FAF
			)), unchecked((int)(0x96D89736)), unchecked((int)(0x857A0F41)), unchecked((int)(
			0x620305BC)), unchecked((int)(0x71A19DCB)), unchecked((int)(0x45463552)), unchecked(
			(int)(0x56E4AD25)), unchecked((int)(0x2C896460)), unchecked((int)(0x3F2BFC17)), 
			unchecked((int)(0x0BCC548E)), unchecked((int)(0x186ECCF9)), unchecked((int)(0xC0D23785
			)), unchecked((int)(0xD370AFF2)), unchecked((int)(0xE797076B)), unchecked((int)(
			0xF4359F1C)), unchecked((int)(0x8E585659)), unchecked((int)(0x9DFACE2E)), unchecked(
			(int)(0xA91D66B7)), unchecked((int)(0xBABFFEC0)), unchecked((int)(0x5DC6F43D)), 
			unchecked((int)(0x4E646C4A)), unchecked((int)(0x7A83C4D3)), unchecked((int)(0x69215CA4
			)), unchecked((int)(0x134C95E1)), unchecked((int)(0x00EE0D96)), unchecked((int)(
			0x3409A50F)), unchecked((int)(0x27AB3D78)), unchecked((int)(0x809C2506)), unchecked(
			(int)(0x933EBD71)), unchecked((int)(0xA7D915E8)), unchecked((int)(0xB47B8D9F)), 
			unchecked((int)(0xCE1644DA)), unchecked((int)(0xDDB4DCAD)), unchecked((int)(0xE9537434
			)), unchecked((int)(0xFAF1EC43)), unchecked((int)(0x1D88E6BE)), unchecked((int)(
			0x0E2A7EC9)), unchecked((int)(0x3ACDD650)), unchecked((int)(0x296F4E27)), unchecked(
			(int)(0x53028762)), unchecked((int)(0x40A01F15)), unchecked((int)(0x7447B78C)), 
			unchecked((int)(0x67E52FFB)), unchecked((int)(0xBF59D487)), unchecked((int)(0xACFB4CF0
			)), unchecked((int)(0x981CE469)), unchecked((int)(0x8BBE7C1E)), unchecked((int)(
			0xF1D3B55B)), unchecked((int)(0xE2712D2C)), unchecked((int)(0xD69685B5)), unchecked(
			(int)(0xC5341DC2)), unchecked((int)(0x224D173F)), unchecked((int)(0x31EF8F48)), 
			unchecked((int)(0x050827D1)), unchecked((int)(0x16AABFA6)), unchecked((int)(0x6CC776E3
			)), unchecked((int)(0x7F65EE94)), unchecked((int)(0x4B82460D)), unchecked((int)(
			0x5820DE7A)), unchecked((int)(0xFBC3FAF9)), unchecked((int)(0xE861628E)), unchecked(
			(int)(0xDC86CA17)), unchecked((int)(0xCF245260)), unchecked((int)(0xB5499B25)), 
			unchecked((int)(0xA6EB0352)), unchecked((int)(0x920CABCB)), unchecked((int)(0x81AE33BC
			)), unchecked((int)(0x66D73941)), unchecked((int)(0x7575A136)), unchecked((int)(
			0x419209AF)), unchecked((int)(0x523091D8)), unchecked((int)(0x285D589D)), unchecked(
			(int)(0x3BFFC0EA)), unchecked((int)(0x0F186873)), unchecked((int)(0x1CBAF004)), 
			unchecked((int)(0xC4060B78)), unchecked((int)(0xD7A4930F)), unchecked((int)(0xE3433B96
			)), unchecked((int)(0xF0E1A3E1)), unchecked((int)(0x8A8C6AA4)), unchecked((int)(
			0x992EF2D3)), unchecked((int)(0xADC95A4A)), unchecked((int)(0xBE6BC23D)), unchecked(
			(int)(0x5912C8C0)), unchecked((int)(0x4AB050B7)), unchecked((int)(0x7E57F82E)), 
			unchecked((int)(0x6DF56059)), unchecked((int)(0x1798A91C)), unchecked((int)(0x043A316B
			)), unchecked((int)(0x30DD99F2)), unchecked((int)(0x237F0185)), unchecked((int)(
			0x844819FB)), unchecked((int)(0x97EA818C)), unchecked((int)(0xA30D2915)), unchecked(
			(int)(0xB0AFB162)), unchecked((int)(0xCAC27827)), unchecked((int)(0xD960E050)), 
			unchecked((int)(0xED8748C9)), unchecked((int)(0xFE25D0BE)), unchecked((int)(0x195CDA43
			)), unchecked((int)(0x0AFE4234)), unchecked((int)(0x3E19EAAD)), unchecked((int)(
			0x2DBB72DA)), unchecked((int)(0x57D6BB9F)), unchecked((int)(0x447423E8)), unchecked(
			(int)(0x70938B71)), unchecked((int)(0x63311306)), unchecked((int)(0xBB8DE87A)), 
			unchecked((int)(0xA82F700D)), unchecked((int)(0x9CC8D894)), unchecked((int)(0x8F6A40E3
			)), unchecked((int)(0xF50789A6)), unchecked((int)(0xE6A511D1)), unchecked((int)(
			0xD242B948)), unchecked((int)(0xC1E0213F)), unchecked((int)(0x26992BC2)), unchecked(
			(int)(0x353BB3B5)), unchecked((int)(0x01DC1B2C)), unchecked((int)(0x127E835B)), 
			unchecked((int)(0x68134A1E)), unchecked((int)(0x7BB1D269)), unchecked((int)(0x4F567AF0
			)), unchecked((int)(0x5CF4E287)), unchecked((int)(0x04D43CFD)), unchecked((int)(
			0x1776A48A)), unchecked((int)(0x23910C13)), unchecked((int)(0x30339464)), unchecked(
			(int)(0x4A5E5D21)), unchecked((int)(0x59FCC556)), unchecked((int)(0x6D1B6DCF)), 
			unchecked((int)(0x7EB9F5B8)), unchecked((int)(0x99C0FF45)), unchecked((int)(0x8A626732
			)), unchecked((int)(0xBE85CFAB)), unchecked((int)(0xAD2757DC)), unchecked((int)(
			0xD74A9E99)), unchecked((int)(0xC4E806EE)), unchecked((int)(0xF00FAE77)), unchecked(
			(int)(0xE3AD3600)), unchecked((int)(0x3B11CD7C)), unchecked((int)(0x28B3550B)), 
			unchecked((int)(0x1C54FD92)), unchecked((int)(0x0FF665E5)), unchecked((int)(0x759BACA0
			)), unchecked((int)(0x663934D7)), unchecked((int)(0x52DE9C4E)), unchecked((int)(
			0x417C0439)), unchecked((int)(0xA6050EC4)), unchecked((int)(0xB5A796B3)), unchecked(
			(int)(0x81403E2A)), unchecked((int)(0x92E2A65D)), unchecked((int)(0xE88F6F18)), 
			unchecked((int)(0xFB2DF76F)), unchecked((int)(0xCFCA5FF6)), unchecked((int)(0xDC68C781
			)), unchecked((int)(0x7B5FDFFF)), unchecked((int)(0x68FD4788)), unchecked((int)(
			0x5C1AEF11)), unchecked((int)(0x4FB87766)), unchecked((int)(0x35D5BE23)), unchecked(
			(int)(0x26772654)), unchecked((int)(0x12908ECD)), unchecked((int)(0x013216BA)), 
			unchecked((int)(0xE64B1C47)), unchecked((int)(0xF5E98430)), unchecked((int)(0xC10E2CA9
			)), unchecked((int)(0xD2ACB4DE)), unchecked((int)(0xA8C17D9B)), unchecked((int)(
			0xBB63E5EC)), unchecked((int)(0x8F844D75)), unchecked((int)(0x9C26D502)), unchecked(
			(int)(0x449A2E7E)), unchecked((int)(0x5738B609)), unchecked((int)(0x63DF1E90)), 
			unchecked((int)(0x707D86E7)), unchecked((int)(0x0A104FA2)), unchecked((int)(0x19B2D7D5
			)), unchecked((int)(0x2D557F4C)), unchecked((int)(0x3EF7E73B)), unchecked((int)(
			0xD98EEDC6)), unchecked((int)(0xCA2C75B1)), unchecked((int)(0xFECBDD28)), unchecked(
			(int)(0xED69455F)), unchecked((int)(0x97048C1A)), unchecked((int)(0x84A6146D)), 
			unchecked((int)(0xB041BCF4)), unchecked((int)(0xA3E32483)), unchecked((int)(0x00000000
			)), unchecked((int)(0xA541927E)), unchecked((int)(0x4F6F520D)), unchecked((int)(
			0xEA2EC073)), unchecked((int)(0x9EDEA41A)), unchecked((int)(0x3B9F3664)), unchecked(
			(int)(0xD1B1F617)), unchecked((int)(0x74F06469)), unchecked((int)(0x38513EC5)), 
			unchecked((int)(0x9D10ACBB)), unchecked((int)(0x773E6CC8)), unchecked((int)(0xD27FFEB6
			)), unchecked((int)(0xA68F9ADF)), unchecked((int)(0x03CE08A1)), unchecked((int)(
			0xE9E0C8D2)), unchecked((int)(0x4CA15AAC)), unchecked((int)(0x70A27D8A)), unchecked(
			(int)(0xD5E3EFF4)), unchecked((int)(0x3FCD2F87)), unchecked((int)(0x9A8CBDF9)), 
			unchecked((int)(0xEE7CD990)), unchecked((int)(0x4B3D4BEE)), unchecked((int)(0xA1138B9D
			)), unchecked((int)(0x045219E3)), unchecked((int)(0x48F3434F)), unchecked((int)(
			0xEDB2D131)), unchecked((int)(0x079C1142)), unchecked((int)(0xA2DD833C)), unchecked(
			(int)(0xD62DE755)), unchecked((int)(0x736C752B)), unchecked((int)(0x9942B558)), 
			unchecked((int)(0x3C032726)), unchecked((int)(0xE144FB14)), unchecked((int)(0x4405696A
			)), unchecked((int)(0xAE2BA919)), unchecked((int)(0x0B6A3B67)), unchecked((int)(
			0x7F9A5F0E)), unchecked((int)(0xDADBCD70)), unchecked((int)(0x30F50D03)), unchecked(
			(int)(0x95B49F7D)), unchecked((int)(0xD915C5D1)), unchecked((int)(0x7C5457AF)), 
			unchecked((int)(0x967A97DC)), unchecked((int)(0x333B05A2)), unchecked((int)(0x47CB61CB
			)), unchecked((int)(0xE28AF3B5)), unchecked((int)(0x08A433C6)), unchecked((int)(
			0xADE5A1B8)), unchecked((int)(0x91E6869E)), unchecked((int)(0x34A714E0)), unchecked(
			(int)(0xDE89D493)), unchecked((int)(0x7BC846ED)), unchecked((int)(0x0F382284)), 
			unchecked((int)(0xAA79B0FA)), unchecked((int)(0x40577089)), unchecked((int)(0xE516E2F7
			)), unchecked((int)(0xA9B7B85B)), unchecked((int)(0x0CF62A25)), unchecked((int)(
			0xE6D8EA56)), unchecked((int)(0x43997828)), unchecked((int)(0x37691C41)), unchecked(
			(int)(0x92288E3F)), unchecked((int)(0x78064E4C)), unchecked((int)(0xDD47DC32)), 
			unchecked((int)(0xC76580D9)), unchecked((int)(0x622412A7)), unchecked((int)(0x880AD2D4
			)), unchecked((int)(0x2D4B40AA)), unchecked((int)(0x59BB24C3)), unchecked((int)(
			0xFCFAB6BD)), unchecked((int)(0x16D476CE)), unchecked((int)(0xB395E4B0)), unchecked(
			(int)(0xFF34BE1C)), unchecked((int)(0x5A752C62)), unchecked((int)(0xB05BEC11)), 
			unchecked((int)(0x151A7E6F)), unchecked((int)(0x61EA1A06)), unchecked((int)(0xC4AB8878
			)), unchecked((int)(0x2E85480B)), unchecked((int)(0x8BC4DA75)), unchecked((int)(
			0xB7C7FD53)), unchecked((int)(0x12866F2D)), unchecked((int)(0xF8A8AF5E)), unchecked(
			(int)(0x5DE93D20)), unchecked((int)(0x29195949)), unchecked((int)(0x8C58CB37)), 
			unchecked((int)(0x66760B44)), unchecked((int)(0xC337993A)), unchecked((int)(0x8F96C396
			)), unchecked((int)(0x2AD751E8)), unchecked((int)(0xC0F9919B)), unchecked((int)(
			0x65B803E5)), unchecked((int)(0x1148678C)), unchecked((int)(0xB409F5F2)), unchecked(
			(int)(0x5E273581)), unchecked((int)(0xFB66A7FF)), unchecked((int)(0x26217BCD)), 
			unchecked((int)(0x8360E9B3)), unchecked((int)(0x694E29C0)), unchecked((int)(0xCC0FBBBE
			)), unchecked((int)(0xB8FFDFD7)), unchecked((int)(0x1DBE4DA9)), unchecked((int)(
			0xF7908DDA)), unchecked((int)(0x52D11FA4)), unchecked((int)(0x1E704508)), unchecked(
			(int)(0xBB31D776)), unchecked((int)(0x511F1705)), unchecked((int)(0xF45E857B)), 
			unchecked((int)(0x80AEE112)), unchecked((int)(0x25EF736C)), unchecked((int)(0xCFC1B31F
			)), unchecked((int)(0x6A802161)), unchecked((int)(0x56830647)), unchecked((int)(
			0xF3C29439)), unchecked((int)(0x19EC544A)), unchecked((int)(0xBCADC634)), unchecked(
			(int)(0xC85DA25D)), unchecked((int)(0x6D1C3023)), unchecked((int)(0x8732F050)), 
			unchecked((int)(0x2273622E)), unchecked((int)(0x6ED23882)), unchecked((int)(0xCB93AAFC
			)), unchecked((int)(0x21BD6A8F)), unchecked((int)(0x84FCF8F1)), unchecked((int)(
			0xF00C9C98)), unchecked((int)(0x554D0EE6)), unchecked((int)(0xBF63CE95)), unchecked(
			(int)(0x1A225CEB)), unchecked((int)(0x8B277743)), unchecked((int)(0x2E66E53D)), 
			unchecked((int)(0xC448254E)), unchecked((int)(0x6109B730)), unchecked((int)(0x15F9D359
			)), unchecked((int)(0xB0B84127)), unchecked((int)(0x5A968154)), unchecked((int)(
			0xFFD7132A)), unchecked((int)(0xB3764986)), unchecked((int)(0x1637DBF8)), unchecked(
			(int)(0xFC191B8B)), unchecked((int)(0x595889F5)), unchecked((int)(0x2DA8ED9C)), 
			unchecked((int)(0x88E97FE2)), unchecked((int)(0x62C7BF91)), unchecked((int)(0xC7862DEF
			)), unchecked((int)(0xFB850AC9)), unchecked((int)(0x5EC498B7)), unchecked((int)(
			0xB4EA58C4)), unchecked((int)(0x11ABCABA)), unchecked((int)(0x655BAED3)), unchecked(
			(int)(0xC01A3CAD)), unchecked((int)(0x2A34FCDE)), unchecked((int)(0x8F756EA0)), 
			unchecked((int)(0xC3D4340C)), unchecked((int)(0x6695A672)), unchecked((int)(0x8CBB6601
			)), unchecked((int)(0x29FAF47F)), unchecked((int)(0x5D0A9016)), unchecked((int)(
			0xF84B0268)), unchecked((int)(0x1265C21B)), unchecked((int)(0xB7245065)), unchecked(
			(int)(0x6A638C57)), unchecked((int)(0xCF221E29)), unchecked((int)(0x250CDE5A)), 
			unchecked((int)(0x804D4C24)), unchecked((int)(0xF4BD284D)), unchecked((int)(0x51FCBA33
			)), unchecked((int)(0xBBD27A40)), unchecked((int)(0x1E93E83E)), unchecked((int)(
			0x5232B292)), unchecked((int)(0xF77320EC)), unchecked((int)(0x1D5DE09F)), unchecked(
			(int)(0xB81C72E1)), unchecked((int)(0xCCEC1688)), unchecked((int)(0x69AD84F6)), 
			unchecked((int)(0x83834485)), unchecked((int)(0x26C2D6FB)), unchecked((int)(0x1AC1F1DD
			)), unchecked((int)(0xBF8063A3)), unchecked((int)(0x55AEA3D0)), unchecked((int)(
			0xF0EF31AE)), unchecked((int)(0x841F55C7)), unchecked((int)(0x215EC7B9)), unchecked(
			(int)(0xCB7007CA)), unchecked((int)(0x6E3195B4)), unchecked((int)(0x2290CF18)), 
			unchecked((int)(0x87D15D66)), unchecked((int)(0x6DFF9D15)), unchecked((int)(0xC8BE0F6B
			)), unchecked((int)(0xBC4E6B02)), unchecked((int)(0x190FF97C)), unchecked((int)(
			0xF321390F)), unchecked((int)(0x5660AB71)), unchecked((int)(0x4C42F79A)), unchecked(
			(int)(0xE90365E4)), unchecked((int)(0x032DA597)), unchecked((int)(0xA66C37E9)), 
			unchecked((int)(0xD29C5380)), unchecked((int)(0x77DDC1FE)), unchecked((int)(0x9DF3018D
			)), unchecked((int)(0x38B293F3)), unchecked((int)(0x7413C95F)), unchecked((int)(
			0xD1525B21)), unchecked((int)(0x3B7C9B52)), unchecked((int)(0x9E3D092C)), unchecked(
			(int)(0xEACD6D45)), unchecked((int)(0x4F8CFF3B)), unchecked((int)(0xA5A23F48)), 
			unchecked((int)(0x00E3AD36)), unchecked((int)(0x3CE08A10)), unchecked((int)(0x99A1186E
			)), unchecked((int)(0x738FD81D)), unchecked((int)(0xD6CE4A63)), unchecked((int)(
			0xA23E2E0A)), unchecked((int)(0x077FBC74)), unchecked((int)(0xED517C07)), unchecked(
			(int)(0x4810EE79)), unchecked((int)(0x04B1B4D5)), unchecked((int)(0xA1F026AB)), 
			unchecked((int)(0x4BDEE6D8)), unchecked((int)(0xEE9F74A6)), unchecked((int)(0x9A6F10CF
			)), unchecked((int)(0x3F2E82B1)), unchecked((int)(0xD50042C2)), unchecked((int)(
			0x7041D0BC)), unchecked((int)(0xAD060C8E)), unchecked((int)(0x08479EF0)), unchecked(
			(int)(0xE2695E83)), unchecked((int)(0x4728CCFD)), unchecked((int)(0x33D8A894)), 
			unchecked((int)(0x96993AEA)), unchecked((int)(0x7CB7FA99)), unchecked((int)(0xD9F668E7
			)), unchecked((int)(0x9557324B)), unchecked((int)(0x3016A035)), unchecked((int)(
			0xDA386046)), unchecked((int)(0x7F79F238)), unchecked((int)(0x0B899651)), unchecked(
			(int)(0xAEC8042F)), unchecked((int)(0x44E6C45C)), unchecked((int)(0xE1A75622)), 
			unchecked((int)(0xDDA47104)), unchecked((int)(0x78E5E37A)), unchecked((int)(0x92CB2309
			)), unchecked((int)(0x378AB177)), unchecked((int)(0x437AD51E)), unchecked((int)(
			0xE63B4760)), unchecked((int)(0x0C158713)), unchecked((int)(0xA954156D)), unchecked(
			(int)(0xE5F54FC1)), unchecked((int)(0x40B4DDBF)), unchecked((int)(0xAA9A1DCC)), 
			unchecked((int)(0x0FDB8FB2)), unchecked((int)(0x7B2BEBDB)), unchecked((int)(0xDE6A79A5
			)), unchecked((int)(0x3444B9D6)), unchecked((int)(0x91052BA8)), unchecked((int)(
			0x00000000)), unchecked((int)(0xDD45AAB8)), unchecked((int)(0xBF672381)), unchecked(
			(int)(0x62228939)), unchecked((int)(0x7B2231F3)), unchecked((int)(0xA6679B4B)), 
			unchecked((int)(0xC4451272)), unchecked((int)(0x1900B8CA)), unchecked((int)(0xF64463E6
			)), unchecked((int)(0x2B01C95E)), unchecked((int)(0x49234067)), unchecked((int)(
			0x9466EADF)), unchecked((int)(0x8D665215)), unchecked((int)(0x5023F8AD)), unchecked(
			(int)(0x32017194)), unchecked((int)(0xEF44DB2C)), unchecked((int)(0xE964B13D)), 
			unchecked((int)(0x34211B85)), unchecked((int)(0x560392BC)), unchecked((int)(0x8B463804
			)), unchecked((int)(0x924680CE)), unchecked((int)(0x4F032A76)), unchecked((int)(
			0x2D21A34F)), unchecked((int)(0xF06409F7)), unchecked((int)(0x1F20D2DB)), unchecked(
			(int)(0xC2657863)), unchecked((int)(0xA047F15A)), unchecked((int)(0x7D025BE2)), 
			unchecked((int)(0x6402E328)), unchecked((int)(0xB9474990)), unchecked((int)(0xDB65C0A9
			)), unchecked((int)(0x06206A11)), unchecked((int)(0xD725148B)), unchecked((int)(
			0x0A60BE33)), unchecked((int)(0x6842370A)), unchecked((int)(0xB5079DB2)), unchecked(
			(int)(0xAC072578)), unchecked((int)(0x71428FC0)), unchecked((int)(0x136006F9)), 
			unchecked((int)(0xCE25AC41)), unchecked((int)(0x2161776D)), unchecked((int)(0xFC24DDD5
			)), unchecked((int)(0x9E0654EC)), unchecked((int)(0x4343FE54)), unchecked((int)(
			0x5A43469E)), unchecked((int)(0x8706EC26)), unchecked((int)(0xE524651F)), unchecked(
			(int)(0x3861CFA7)), unchecked((int)(0x3E41A5B6)), unchecked((int)(0xE3040F0E)), 
			unchecked((int)(0x81268637)), unchecked((int)(0x5C632C8F)), unchecked((int)(0x45639445
			)), unchecked((int)(0x98263EFD)), unchecked((int)(0xFA04B7C4)), unchecked((int)(
			0x27411D7C)), unchecked((int)(0xC805C650)), unchecked((int)(0x15406CE8)), unchecked(
			(int)(0x7762E5D1)), unchecked((int)(0xAA274F69)), unchecked((int)(0xB327F7A3)), 
			unchecked((int)(0x6E625D1B)), unchecked((int)(0x0C40D422)), unchecked((int)(0xD1057E9A
			)), unchecked((int)(0xABA65FE7)), unchecked((int)(0x76E3F55F)), unchecked((int)(
			0x14C17C66)), unchecked((int)(0xC984D6DE)), unchecked((int)(0xD0846E14)), unchecked(
			(int)(0x0DC1C4AC)), unchecked((int)(0x6FE34D95)), unchecked((int)(0xB2A6E72D)), 
			unchecked((int)(0x5DE23C01)), unchecked((int)(0x80A796B9)), unchecked((int)(0xE2851F80
			)), unchecked((int)(0x3FC0B538)), unchecked((int)(0x26C00DF2)), unchecked((int)(
			0xFB85A74A)), unchecked((int)(0x99A72E73)), unchecked((int)(0x44E284CB)), unchecked(
			(int)(0x42C2EEDA)), unchecked((int)(0x9F874462)), unchecked((int)(0xFDA5CD5B)), 
			unchecked((int)(0x20E067E3)), unchecked((int)(0x39E0DF29)), unchecked((int)(0xE4A57591
			)), unchecked((int)(0x8687FCA8)), unchecked((int)(0x5BC25610)), unchecked((int)(
			0xB4868D3C)), unchecked((int)(0x69C32784)), unchecked((int)(0x0BE1AEBD)), unchecked(
			(int)(0xD6A40405)), unchecked((int)(0xCFA4BCCF)), unchecked((int)(0x12E11677)), 
			unchecked((int)(0x70C39F4E)), unchecked((int)(0xAD8635F6)), unchecked((int)(0x7C834B6C
			)), unchecked((int)(0xA1C6E1D4)), unchecked((int)(0xC3E468ED)), unchecked((int)(
			0x1EA1C255)), unchecked((int)(0x07A17A9F)), unchecked((int)(0xDAE4D027)), unchecked(
			(int)(0xB8C6591E)), unchecked((int)(0x6583F3A6)), unchecked((int)(0x8AC7288A)), 
			unchecked((int)(0x57828232)), unchecked((int)(0x35A00B0B)), unchecked((int)(0xE8E5A1B3
			)), unchecked((int)(0xF1E51979)), unchecked((int)(0x2CA0B3C1)), unchecked((int)(
			0x4E823AF8)), unchecked((int)(0x93C79040)), unchecked((int)(0x95E7FA51)), unchecked(
			(int)(0x48A250E9)), unchecked((int)(0x2A80D9D0)), unchecked((int)(0xF7C57368)), 
			unchecked((int)(0xEEC5CBA2)), unchecked((int)(0x3380611A)), unchecked((int)(0x51A2E823
			)), unchecked((int)(0x8CE7429B)), unchecked((int)(0x63A399B7)), unchecked((int)(
			0xBEE6330F)), unchecked((int)(0xDCC4BA36)), unchecked((int)(0x0181108E)), unchecked(
			(int)(0x1881A844)), unchecked((int)(0xC5C402FC)), unchecked((int)(0xA7E68BC5)), 
			unchecked((int)(0x7AA3217D)), unchecked((int)(0x52A0C93F)), unchecked((int)(0x8FE56387
			)), unchecked((int)(0xEDC7EABE)), unchecked((int)(0x30824006)), unchecked((int)(
			0x2982F8CC)), unchecked((int)(0xF4C75274)), unchecked((int)(0x96E5DB4D)), unchecked(
			(int)(0x4BA071F5)), unchecked((int)(0xA4E4AAD9)), unchecked((int)(0x79A10061)), 
			unchecked((int)(0x1B838958)), unchecked((int)(0xC6C623E0)), unchecked((int)(0xDFC69B2A
			)), unchecked((int)(0x02833192)), unchecked((int)(0x60A1B8AB)), unchecked((int)(
			0xBDE41213)), unchecked((int)(0xBBC47802)), unchecked((int)(0x6681D2BA)), unchecked(
			(int)(0x04A35B83)), unchecked((int)(0xD9E6F13B)), unchecked((int)(0xC0E649F1)), 
			unchecked((int)(0x1DA3E349)), unchecked((int)(0x7F816A70)), unchecked((int)(0xA2C4C0C8
			)), unchecked((int)(0x4D801BE4)), unchecked((int)(0x90C5B15C)), unchecked((int)(
			0xF2E73865)), unchecked((int)(0x2FA292DD)), unchecked((int)(0x36A22A17)), unchecked(
			(int)(0xEBE780AF)), unchecked((int)(0x89C50996)), unchecked((int)(0x5480A32E)), 
			unchecked((int)(0x8585DDB4)), unchecked((int)(0x58C0770C)), unchecked((int)(0x3AE2FE35
			)), unchecked((int)(0xE7A7548D)), unchecked((int)(0xFEA7EC47)), unchecked((int)(
			0x23E246FF)), unchecked((int)(0x41C0CFC6)), unchecked((int)(0x9C85657E)), unchecked(
			(int)(0x73C1BE52)), unchecked((int)(0xAE8414EA)), unchecked((int)(0xCCA69DD3)), 
			unchecked((int)(0x11E3376B)), unchecked((int)(0x08E38FA1)), unchecked((int)(0xD5A62519
			)), unchecked((int)(0xB784AC20)), unchecked((int)(0x6AC10698)), unchecked((int)(
			0x6CE16C89)), unchecked((int)(0xB1A4C631)), unchecked((int)(0xD3864F08)), unchecked(
			(int)(0x0EC3E5B0)), unchecked((int)(0x17C35D7A)), unchecked((int)(0xCA86F7C2)), 
			unchecked((int)(0xA8A47EFB)), unchecked((int)(0x75E1D443)), unchecked((int)(0x9AA50F6F
			)), unchecked((int)(0x47E0A5D7)), unchecked((int)(0x25C22CEE)), unchecked((int)(
			0xF8878656)), unchecked((int)(0xE1873E9C)), unchecked((int)(0x3CC29424)), unchecked(
			(int)(0x5EE01D1D)), unchecked((int)(0x83A5B7A5)), unchecked((int)(0xF90696D8)), 
			unchecked((int)(0x24433C60)), unchecked((int)(0x4661B559)), unchecked((int)(0x9B241FE1
			)), unchecked((int)(0x8224A72B)), unchecked((int)(0x5F610D93)), unchecked((int)(
			0x3D4384AA)), unchecked((int)(0xE0062E12)), unchecked((int)(0x0F42F53E)), unchecked(
			(int)(0xD2075F86)), unchecked((int)(0xB025D6BF)), unchecked((int)(0x6D607C07)), 
			unchecked((int)(0x7460C4CD)), unchecked((int)(0xA9256E75)), unchecked((int)(0xCB07E74C
			)), unchecked((int)(0x16424DF4)), unchecked((int)(0x106227E5)), unchecked((int)(
			0xCD278D5D)), unchecked((int)(0xAF050464)), unchecked((int)(0x7240AEDC)), unchecked(
			(int)(0x6B401616)), unchecked((int)(0xB605BCAE)), unchecked((int)(0xD4273597)), 
			unchecked((int)(0x09629F2F)), unchecked((int)(0xE6264403)), unchecked((int)(0x3B63EEBB
			)), unchecked((int)(0x59416782)), unchecked((int)(0x8404CD3A)), unchecked((int)(
			0x9D0475F0)), unchecked((int)(0x4041DF48)), unchecked((int)(0x22635671)), unchecked(
			(int)(0xFF26FCC9)), unchecked((int)(0x2E238253)), unchecked((int)(0xF36628EB)), 
			unchecked((int)(0x9144A1D2)), unchecked((int)(0x4C010B6A)), unchecked((int)(0x5501B3A0
			)), unchecked((int)(0x88441918)), unchecked((int)(0xEA669021)), unchecked((int)(
			0x37233A99)), unchecked((int)(0xD867E1B5)), unchecked((int)(0x05224B0D)), unchecked(
			(int)(0x6700C234)), unchecked((int)(0xBA45688C)), unchecked((int)(0xA345D046)), 
			unchecked((int)(0x7E007AFE)), unchecked((int)(0x1C22F3C7)), unchecked((int)(0xC167597F
			)), unchecked((int)(0xC747336E)), unchecked((int)(0x1A0299D6)), unchecked((int)(
			0x782010EF)), unchecked((int)(0xA565BA57)), unchecked((int)(0xBC65029D)), unchecked(
			(int)(0x6120A825)), unchecked((int)(0x0302211C)), unchecked((int)(0xDE478BA4)), 
			unchecked((int)(0x31035088)), unchecked((int)(0xEC46FA30)), unchecked((int)(0x8E647309
			)), unchecked((int)(0x5321D9B1)), unchecked((int)(0x4A21617B)), unchecked((int)(
			0x9764CBC3)), unchecked((int)(0xF54642FA)), unchecked((int)(0x2803E842)), unchecked(
			(int)(0x00000000)), unchecked((int)(0x38116FAC)), unchecked((int)(0x7022DF58)), 
			unchecked((int)(0x4833B0F4)), unchecked((int)(0xE045BEB0)), unchecked((int)(0xD854D11C
			)), unchecked((int)(0x906761E8)), unchecked((int)(0xA8760E44)), unchecked((int)(
			0xC5670B91)), unchecked((int)(0xFD76643D)), unchecked((int)(0xB545D4C9)), unchecked(
			(int)(0x8D54BB65)), unchecked((int)(0x2522B521)), unchecked((int)(0x1D33DA8D)), 
			unchecked((int)(0x55006A79)), unchecked((int)(0x6D1105D5)), unchecked((int)(0x8F2261D3
			)), unchecked((int)(0xB7330E7F)), unchecked((int)(0xFF00BE8B)), unchecked((int)(
			0xC711D127)), unchecked((int)(0x6F67DF63)), unchecked((int)(0x5776B0CF)), unchecked(
			(int)(0x1F45003B)), unchecked((int)(0x27546F97)), unchecked((int)(0x4A456A42)), 
			unchecked((int)(0x725405EE)), unchecked((int)(0x3A67B51A)), unchecked((int)(0x0276DAB6
			)), unchecked((int)(0xAA00D4F2)), unchecked((int)(0x9211BB5E)), unchecked((int)(
			0xDA220BAA)), unchecked((int)(0xE2336406)), unchecked((int)(0x1BA8B557)), unchecked(
			(int)(0x23B9DAFB)), unchecked((int)(0x6B8A6A0F)), unchecked((int)(0x539B05A3)), 
			unchecked((int)(0xFBED0BE7)), unchecked((int)(0xC3FC644B)), unchecked((int)(0x8BCFD4BF
			)), unchecked((int)(0xB3DEBB13)), unchecked((int)(0xDECFBEC6)), unchecked((int)(
			0xE6DED16A)), unchecked((int)(0xAEED619E)), unchecked((int)(0x96FC0E32)), unchecked(
			(int)(0x3E8A0076)), unchecked((int)(0x069B6FDA)), unchecked((int)(0x4EA8DF2E)), 
			unchecked((int)(0x76B9B082)), unchecked((int)(0x948AD484)), unchecked((int)(0xAC9BBB28
			)), unchecked((int)(0xE4A80BDC)), unchecked((int)(0xDCB96470)), unchecked((int)(
			0x74CF6A34)), unchecked((int)(0x4CDE0598)), unchecked((int)(0x04EDB56C)), unchecked(
			(int)(0x3CFCDAC0)), unchecked((int)(0x51EDDF15)), unchecked((int)(0x69FCB0B9)), 
			unchecked((int)(0x21CF004D)), unchecked((int)(0x19DE6FE1)), unchecked((int)(0xB1A861A5
			)), unchecked((int)(0x89B90E09)), unchecked((int)(0xC18ABEFD)), unchecked((int)(
			0xF99BD151)), unchecked((int)(0x37516AAE)), unchecked((int)(0x0F400502)), unchecked(
			(int)(0x4773B5F6)), unchecked((int)(0x7F62DA5A)), unchecked((int)(0xD714D41E)), 
			unchecked((int)(0xEF05BBB2)), unchecked((int)(0xA7360B46)), unchecked((int)(0x9F2764EA
			)), unchecked((int)(0xF236613F)), unchecked((int)(0xCA270E93)), unchecked((int)(
			0x8214BE67)), unchecked((int)(0xBA05D1CB)), unchecked((int)(0x1273DF8F)), unchecked(
			(int)(0x2A62B023)), unchecked((int)(0x625100D7)), unchecked((int)(0x5A406F7B)), 
			unchecked((int)(0xB8730B7D)), unchecked((int)(0x806264D1)), unchecked((int)(0xC851D425
			)), unchecked((int)(0xF040BB89)), unchecked((int)(0x5836B5CD)), unchecked((int)(
			0x6027DA61)), unchecked((int)(0x28146A95)), unchecked((int)(0x10050539)), unchecked(
			(int)(0x7D1400EC)), unchecked((int)(0x45056F40)), unchecked((int)(0x0D36DFB4)), 
			unchecked((int)(0x3527B018)), unchecked((int)(0x9D51BE5C)), unchecked((int)(0xA540D1F0
			)), unchecked((int)(0xED736104)), unchecked((int)(0xD5620EA8)), unchecked((int)(
			0x2CF9DFF9)), unchecked((int)(0x14E8B055)), unchecked((int)(0x5CDB00A1)), unchecked(
			(int)(0x64CA6F0D)), unchecked((int)(0xCCBC6149)), unchecked((int)(0xF4AD0EE5)), 
			unchecked((int)(0xBC9EBE11)), unchecked((int)(0x848FD1BD)), unchecked((int)(0xE99ED468
			)), unchecked((int)(0xD18FBBC4)), unchecked((int)(0x99BC0B30)), unchecked((int)(
			0xA1AD649C)), unchecked((int)(0x09DB6AD8)), unchecked((int)(0x31CA0574)), unchecked(
			(int)(0x79F9B580)), unchecked((int)(0x41E8DA2C)), unchecked((int)(0xA3DBBE2A)), 
			unchecked((int)(0x9BCAD186)), unchecked((int)(0xD3F96172)), unchecked((int)(0xEBE80EDE
			)), unchecked((int)(0x439E009A)), unchecked((int)(0x7B8F6F36)), unchecked((int)(
			0x33BCDFC2)), unchecked((int)(0x0BADB06E)), unchecked((int)(0x66BCB5BB)), unchecked(
			(int)(0x5EADDA17)), unchecked((int)(0x169E6AE3)), unchecked((int)(0x2E8F054F)), 
			unchecked((int)(0x86F90B0B)), unchecked((int)(0xBEE864A7)), unchecked((int)(0xF6DBD453
			)), unchecked((int)(0xCECABBFF)), unchecked((int)(0x6EA2D55C)), unchecked((int)(
			0x56B3BAF0)), unchecked((int)(0x1E800A04)), unchecked((int)(0x269165A8)), unchecked(
			(int)(0x8EE76BEC)), unchecked((int)(0xB6F60440)), unchecked((int)(0xFEC5B4B4)), 
			unchecked((int)(0xC6D4DB18)), unchecked((int)(0xABC5DECD)), unchecked((int)(0x93D4B161
			)), unchecked((int)(0xDBE70195)), unchecked((int)(0xE3F66E39)), unchecked((int)(
			0x4B80607D)), unchecked((int)(0x73910FD1)), unchecked((int)(0x3BA2BF25)), unchecked(
			(int)(0x03B3D089)), unchecked((int)(0xE180B48F)), unchecked((int)(0xD991DB23)), 
			unchecked((int)(0x91A26BD7)), unchecked((int)(0xA9B3047B)), unchecked((int)(0x01C50A3F
			)), unchecked((int)(0x39D46593)), unchecked((int)(0x71E7D567)), unchecked((int)(
			0x49F6BACB)), unchecked((int)(0x24E7BF1E)), unchecked((int)(0x1CF6D0B2)), unchecked(
			(int)(0x54C56046)), unchecked((int)(0x6CD40FEA)), unchecked((int)(0xC4A201AE)), 
			unchecked((int)(0xFCB36E02)), unchecked((int)(0xB480DEF6)), unchecked((int)(0x8C91B15A
			)), unchecked((int)(0x750A600B)), unchecked((int)(0x4D1B0FA7)), unchecked((int)(
			0x0528BF53)), unchecked((int)(0x3D39D0FF)), unchecked((int)(0x954FDEBB)), unchecked(
			(int)(0xAD5EB117)), unchecked((int)(0xE56D01E3)), unchecked((int)(0xDD7C6E4F)), 
			unchecked((int)(0xB06D6B9A)), unchecked((int)(0x887C0436)), unchecked((int)(0xC04FB4C2
			)), unchecked((int)(0xF85EDB6E)), unchecked((int)(0x5028D52A)), unchecked((int)(
			0x6839BA86)), unchecked((int)(0x200A0A72)), unchecked((int)(0x181B65DE)), unchecked(
			(int)(0xFA2801D8)), unchecked((int)(0xC2396E74)), unchecked((int)(0x8A0ADE80)), 
			unchecked((int)(0xB21BB12C)), unchecked((int)(0x1A6DBF68)), unchecked((int)(0x227CD0C4
			)), unchecked((int)(0x6A4F6030)), unchecked((int)(0x525E0F9C)), unchecked((int)(
			0x3F4F0A49)), unchecked((int)(0x075E65E5)), unchecked((int)(0x4F6DD511)), unchecked(
			(int)(0x777CBABD)), unchecked((int)(0xDF0AB4F9)), unchecked((int)(0xE71BDB55)), 
			unchecked((int)(0xAF286BA1)), unchecked((int)(0x9739040D)), unchecked((int)(0x59F3BFF2
			)), unchecked((int)(0x61E2D05E)), unchecked((int)(0x29D160AA)), unchecked((int)(
			0x11C00F06)), unchecked((int)(0xB9B60142)), unchecked((int)(0x81A76EEE)), unchecked(
			(int)(0xC994DE1A)), unchecked((int)(0xF185B1B6)), unchecked((int)(0x9C94B463)), 
			unchecked((int)(0xA485DBCF)), unchecked((int)(0xECB66B3B)), unchecked((int)(0xD4A70497
			)), unchecked((int)(0x7CD10AD3)), unchecked((int)(0x44C0657F)), unchecked((int)(
			0x0CF3D58B)), unchecked((int)(0x34E2BA27)), unchecked((int)(0xD6D1DE21)), unchecked(
			(int)(0xEEC0B18D)), unchecked((int)(0xA6F30179)), unchecked((int)(0x9EE26ED5)), 
			unchecked((int)(0x36946091)), unchecked((int)(0x0E850F3D)), unchecked((int)(0x46B6BFC9
			)), unchecked((int)(0x7EA7D065)), unchecked((int)(0x13B6D5B0)), unchecked((int)(
			0x2BA7BA1C)), unchecked((int)(0x63940AE8)), unchecked((int)(0x5B856544)), unchecked(
			(int)(0xF3F36B00)), unchecked((int)(0xCBE204AC)), unchecked((int)(0x83D1B458)), 
			unchecked((int)(0xBBC0DBF4)), unchecked((int)(0x425B0AA5)), unchecked((int)(0x7A4A6509
			)), unchecked((int)(0x3279D5FD)), unchecked((int)(0x0A68BA51)), unchecked((int)(
			0xA21EB415)), unchecked((int)(0x9A0FDBB9)), unchecked((int)(0xD23C6B4D)), unchecked(
			(int)(0xEA2D04E1)), unchecked((int)(0x873C0134)), unchecked((int)(0xBF2D6E98)), 
			unchecked((int)(0xF71EDE6C)), unchecked((int)(0xCF0FB1C0)), unchecked((int)(0x6779BF84
			)), unchecked((int)(0x5F68D028)), unchecked((int)(0x175B60DC)), unchecked((int)(
			0x2F4A0F70)), unchecked((int)(0xCD796B76)), unchecked((int)(0xF56804DA)), unchecked(
			(int)(0xBD5BB42E)), unchecked((int)(0x854ADB82)), unchecked((int)(0x2D3CD5C6)), 
			unchecked((int)(0x152DBA6A)), unchecked((int)(0x5D1E0A9E)), unchecked((int)(0x650F6532
			)), unchecked((int)(0x081E60E7)), unchecked((int)(0x300F0F4B)), unchecked((int)(
			0x783CBFBF)), unchecked((int)(0x402DD013)), unchecked((int)(0xE85BDE57)), unchecked(
			(int)(0xD04AB1FB)), unchecked((int)(0x9879010F)), unchecked((int)(0xA0686EA3)), 
			unchecked((int)(0x00000000)), unchecked((int)(0xEF306B19)), unchecked((int)(0xDB8CA0C3
			)), unchecked((int)(0x34BCCBDA)), unchecked((int)(0xB2F53777)), unchecked((int)(
			0x5DC55C6E)), unchecked((int)(0x697997B4)), unchecked((int)(0x8649FCAD)), unchecked(
			(int)(0x6006181F)), unchecked((int)(0x8F367306)), unchecked((int)(0xBB8AB8DC)), 
			unchecked((int)(0x54BAD3C5)), unchecked((int)(0xD2F32F68)), unchecked((int)(0x3DC34471
			)), unchecked((int)(0x097F8FAB)), unchecked((int)(0xE64FE4B2)), unchecked((int)(
			0xC00C303E)), unchecked((int)(0x2F3C5B27)), unchecked((int)(0x1B8090FD)), unchecked(
			(int)(0xF4B0FBE4)), unchecked((int)(0x72F90749)), unchecked((int)(0x9DC96C50)), 
			unchecked((int)(0xA975A78A)), unchecked((int)(0x4645CC93)), unchecked((int)(0xA00A2821
			)), unchecked((int)(0x4F3A4338)), unchecked((int)(0x7B8688E2)), unchecked((int)(
			0x94B6E3FB)), unchecked((int)(0x12FF1F56)), unchecked((int)(0xFDCF744F)), unchecked(
			(int)(0xC973BF95)), unchecked((int)(0x2643D48C)), unchecked((int)(0x85F4168D)), 
			unchecked((int)(0x6AC47D94)), unchecked((int)(0x5E78B64E)), unchecked((int)(0xB148DD57
			)), unchecked((int)(0x370121FA)), unchecked((int)(0xD8314AE3)), unchecked((int)(
			0xEC8D8139)), unchecked((int)(0x03BDEA20)), unchecked((int)(0xE5F20E92)), unchecked(
			(int)(0x0AC2658B)), unchecked((int)(0x3E7EAE51)), unchecked((int)(0xD14EC548)), 
			unchecked((int)(0x570739E5)), unchecked((int)(0xB83752FC)), unchecked((int)(0x8C8B9926
			)), unchecked((int)(0x63BBF23F)), unchecked((int)(0x45F826B3)), unchecked((int)(
			0xAAC84DAA)), unchecked((int)(0x9E748670)), unchecked((int)(0x7144ED69)), unchecked(
			(int)(0xF70D11C4)), unchecked((int)(0x183D7ADD)), unchecked((int)(0x2C81B107)), 
			unchecked((int)(0xC3B1DA1E)), unchecked((int)(0x25FE3EAC)), unchecked((int)(0xCACE55B5
			)), unchecked((int)(0xFE729E6F)), unchecked((int)(0x1142F576)), unchecked((int)(
			0x970B09DB)), unchecked((int)(0x783B62C2)), unchecked((int)(0x4C87A918)), unchecked(
			(int)(0xA3B7C201)), unchecked((int)(0x0E045BEB)), unchecked((int)(0xE13430F2)), 
			unchecked((int)(0xD588FB28)), unchecked((int)(0x3AB89031)), unchecked((int)(0xBCF16C9C
			)), unchecked((int)(0x53C10785)), unchecked((int)(0x677DCC5F)), unchecked((int)(
			0x884DA746)), unchecked((int)(0x6E0243F4)), unchecked((int)(0x813228ED)), unchecked(
			(int)(0xB58EE337)), unchecked((int)(0x5ABE882E)), unchecked((int)(0xDCF77483)), 
			unchecked((int)(0x33C71F9A)), unchecked((int)(0x077BD440)), unchecked((int)(0xE84BBF59
			)), unchecked((int)(0xCE086BD5)), unchecked((int)(0x213800CC)), unchecked((int)(
			0x1584CB16)), unchecked((int)(0xFAB4A00F)), unchecked((int)(0x7CFD5CA2)), unchecked(
			(int)(0x93CD37BB)), unchecked((int)(0xA771FC61)), unchecked((int)(0x48419778)), 
			unchecked((int)(0xAE0E73CA)), unchecked((int)(0x413E18D3)), unchecked((int)(0x7582D309
			)), unchecked((int)(0x9AB2B810)), unchecked((int)(0x1CFB44BD)), unchecked((int)(
			0xF3CB2FA4)), unchecked((int)(0xC777E47E)), unchecked((int)(0x28478F67)), unchecked(
			(int)(0x8BF04D66)), unchecked((int)(0x64C0267F)), unchecked((int)(0x507CEDA5)), 
			unchecked((int)(0xBF4C86BC)), unchecked((int)(0x39057A11)), unchecked((int)(0xD6351108
			)), unchecked((int)(0xE289DAD2)), unchecked((int)(0x0DB9B1CB)), unchecked((int)(
			0xEBF65579)), unchecked((int)(0x04C63E60)), unchecked((int)(0x307AF5BA)), unchecked(
			(int)(0xDF4A9EA3)), unchecked((int)(0x5903620E)), unchecked((int)(0xB6330917)), 
			unchecked((int)(0x828FC2CD)), unchecked((int)(0x6DBFA9D4)), unchecked((int)(0x4BFC7D58
			)), unchecked((int)(0xA4CC1641)), unchecked((int)(0x9070DD9B)), unchecked((int)(
			0x7F40B682)), unchecked((int)(0xF9094A2F)), unchecked((int)(0x16392136)), unchecked(
			(int)(0x2285EAEC)), unchecked((int)(0xCDB581F5)), unchecked((int)(0x2BFA6547)), 
			unchecked((int)(0xC4CA0E5E)), unchecked((int)(0xF076C584)), unchecked((int)(0x1F46AE9D
			)), unchecked((int)(0x990F5230)), unchecked((int)(0x763F3929)), unchecked((int)(
			0x4283F2F3)), unchecked((int)(0xADB399EA)), unchecked((int)(0x1C08B7D6)), unchecked(
			(int)(0xF338DCCF)), unchecked((int)(0xC7841715)), unchecked((int)(0x28B47C0C)), 
			unchecked((int)(0xAEFD80A1)), unchecked((int)(0x41CDEBB8)), unchecked((int)(0x75712062
			)), unchecked((int)(0x9A414B7B)), unchecked((int)(0x7C0EAFC9)), unchecked((int)(
			0x933EC4D0)), unchecked((int)(0xA7820F0A)), unchecked((int)(0x48B26413)), unchecked(
			(int)(0xCEFB98BE)), unchecked((int)(0x21CBF3A7)), unchecked((int)(0x1577387D)), 
			unchecked((int)(0xFA475364)), unchecked((int)(0xDC0487E8)), unchecked((int)(0x3334ECF1
			)), unchecked((int)(0x0788272B)), unchecked((int)(0xE8B84C32)), unchecked((int)(
			0x6EF1B09F)), unchecked((int)(0x81C1DB86)), unchecked((int)(0xB57D105C)), unchecked(
			(int)(0x5A4D7B45)), unchecked((int)(0xBC029FF7)), unchecked((int)(0x5332F4EE)), 
			unchecked((int)(0x678E3F34)), unchecked((int)(0x88BE542D)), unchecked((int)(0x0EF7A880
			)), unchecked((int)(0xE1C7C399)), unchecked((int)(0xD57B0843)), unchecked((int)(
			0x3A4B635A)), unchecked((int)(0x99FCA15B)), unchecked((int)(0x76CCCA42)), unchecked(
			(int)(0x42700198)), unchecked((int)(0xAD406A81)), unchecked((int)(0x2B09962C)), 
			unchecked((int)(0xC439FD35)), unchecked((int)(0xF08536EF)), unchecked((int)(0x1FB55DF6
			)), unchecked((int)(0xF9FAB944)), unchecked((int)(0x16CAD25D)), unchecked((int)(
			0x22761987)), unchecked((int)(0xCD46729E)), unchecked((int)(0x4B0F8E33)), unchecked(
			(int)(0xA43FE52A)), unchecked((int)(0x90832EF0)), unchecked((int)(0x7FB345E9)), 
			unchecked((int)(0x59F09165)), unchecked((int)(0xB6C0FA7C)), unchecked((int)(0x827C31A6
			)), unchecked((int)(0x6D4C5ABF)), unchecked((int)(0xEB05A612)), unchecked((int)(
			0x0435CD0B)), unchecked((int)(0x308906D1)), unchecked((int)(0xDFB96DC8)), unchecked(
			(int)(0x39F6897A)), unchecked((int)(0xD6C6E263)), unchecked((int)(0xE27A29B9)), 
			unchecked((int)(0x0D4A42A0)), unchecked((int)(0x8B03BE0D)), unchecked((int)(0x6433D514
			)), unchecked((int)(0x508F1ECE)), unchecked((int)(0xBFBF75D7)), unchecked((int)(
			0x120CEC3D)), unchecked((int)(0xFD3C8724)), unchecked((int)(0xC9804CFE)), unchecked(
			(int)(0x26B027E7)), unchecked((int)(0xA0F9DB4A)), unchecked((int)(0x4FC9B053)), 
			unchecked((int)(0x7B757B89)), unchecked((int)(0x94451090)), unchecked((int)(0x720AF422
			)), unchecked((int)(0x9D3A9F3B)), unchecked((int)(0xA98654E1)), unchecked((int)(
			0x46B63FF8)), unchecked((int)(0xC0FFC355)), unchecked((int)(0x2FCFA84C)), unchecked(
			(int)(0x1B736396)), unchecked((int)(0xF443088F)), unchecked((int)(0xD200DC03)), 
			unchecked((int)(0x3D30B71A)), unchecked((int)(0x098C7CC0)), unchecked((int)(0xE6BC17D9
			)), unchecked((int)(0x60F5EB74)), unchecked((int)(0x8FC5806D)), unchecked((int)(
			0xBB794BB7)), unchecked((int)(0x544920AE)), unchecked((int)(0xB206C41C)), unchecked(
			(int)(0x5D36AF05)), unchecked((int)(0x698A64DF)), unchecked((int)(0x86BA0FC6)), 
			unchecked((int)(0x00F3F36B)), unchecked((int)(0xEFC39872)), unchecked((int)(0xDB7F53A8
			)), unchecked((int)(0x344F38B1)), unchecked((int)(0x97F8FAB0)), unchecked((int)(
			0x78C891A9)), unchecked((int)(0x4C745A73)), unchecked((int)(0xA344316A)), unchecked(
			(int)(0x250DCDC7)), unchecked((int)(0xCA3DA6DE)), unchecked((int)(0xFE816D04)), 
			unchecked((int)(0x11B1061D)), unchecked((int)(0xF7FEE2AF)), unchecked((int)(0x18CE89B6
			)), unchecked((int)(0x2C72426C)), unchecked((int)(0xC3422975)), unchecked((int)(
			0x450BD5D8)), unchecked((int)(0xAA3BBEC1)), unchecked((int)(0x9E87751B)), unchecked(
			(int)(0x71B71E02)), unchecked((int)(0x57F4CA8E)), unchecked((int)(0xB8C4A197)), 
			unchecked((int)(0x8C786A4D)), unchecked((int)(0x63480154)), unchecked((int)(0xE501FDF9
			)), unchecked((int)(0x0A3196E0)), unchecked((int)(0x3E8D5D3A)), unchecked((int)(
			0xD1BD3623)), unchecked((int)(0x37F2D291)), unchecked((int)(0xD8C2B988)), unchecked(
			(int)(0xEC7E7252)), unchecked((int)(0x034E194B)), unchecked((int)(0x8507E5E6)), 
			unchecked((int)(0x6A378EFF)), unchecked((int)(0x5E8B4525)), unchecked((int)(0xB1BB2E3C
			)), unchecked((int)(0x00000000)), unchecked((int)(0x68032CC8)), unchecked((int)(
			0xD0065990)), unchecked((int)(0xB8057558)), unchecked((int)(0xA5E0C5D1)), unchecked(
			(int)(0xCDE3E919)), unchecked((int)(0x75E69C41)), unchecked((int)(0x1DE5B089)), 
			unchecked((int)(0x4E2DFD53)), unchecked((int)(0x262ED19B)), unchecked((int)(0x9E2BA4C3
			)), unchecked((int)(0xF628880B)), unchecked((int)(0xEBCD3882)), unchecked((int)(
			0x83CE144A)), unchecked((int)(0x3BCB6112)), unchecked((int)(0x53C84DDA)), unchecked(
			(int)(0x9C5BFAA6)), unchecked((int)(0xF458D66E)), unchecked((int)(0x4C5DA336)), 
			unchecked((int)(0x245E8FFE)), unchecked((int)(0x39BB3F77)), unchecked((int)(0x51B813BF
			)), unchecked((int)(0xE9BD66E7)), unchecked((int)(0x81BE4A2F)), unchecked((int)(
			0xD27607F5)), unchecked((int)(0xBA752B3D)), unchecked((int)(0x02705E65)), unchecked(
			(int)(0x6A7372AD)), unchecked((int)(0x7796C224)), unchecked((int)(0x1F95EEEC)), 
			unchecked((int)(0xA7909BB4)), unchecked((int)(0xCF93B77C)), unchecked((int)(0x3D5B83BD
			)), unchecked((int)(0x5558AF75)), unchecked((int)(0xED5DDA2D)), unchecked((int)(
			0x855EF6E5)), unchecked((int)(0x98BB466C)), unchecked((int)(0xF0B86AA4)), unchecked(
			(int)(0x48BD1FFC)), unchecked((int)(0x20BE3334)), unchecked((int)(0x73767EEE)), 
			unchecked((int)(0x1B755226)), unchecked((int)(0xA370277E)), unchecked((int)(0xCB730BB6
			)), unchecked((int)(0xD696BB3F)), unchecked((int)(0xBE9597F7)), unchecked((int)(
			0x0690E2AF)), unchecked((int)(0x6E93CE67)), unchecked((int)(0xA100791B)), unchecked(
			(int)(0xC90355D3)), unchecked((int)(0x7106208B)), unchecked((int)(0x19050C43)), 
			unchecked((int)(0x04E0BCCA)), unchecked((int)(0x6CE39002)), unchecked((int)(0xD4E6E55A
			)), unchecked((int)(0xBCE5C992)), unchecked((int)(0xEF2D8448)), unchecked((int)(
			0x872EA880)), unchecked((int)(0x3F2BDDD8)), unchecked((int)(0x5728F110)), unchecked(
			(int)(0x4ACD4199)), unchecked((int)(0x22CE6D51)), unchecked((int)(0x9ACB1809)), 
			unchecked((int)(0xF2C834C1)), unchecked((int)(0x7AB7077A)), unchecked((int)(0x12B42BB2
			)), unchecked((int)(0xAAB15EEA)), unchecked((int)(0xC2B27222)), unchecked((int)(
			0xDF57C2AB)), unchecked((int)(0xB754EE63)), unchecked((int)(0x0F519B3B)), unchecked(
			(int)(0x6752B7F3)), unchecked((int)(0x349AFA29)), unchecked((int)(0x5C99D6E1)), 
			unchecked((int)(0xE49CA3B9)), unchecked((int)(0x8C9F8F71)), unchecked((int)(0x917A3FF8
			)), unchecked((int)(0xF9791330)), unchecked((int)(0x417C6668)), unchecked((int)(
			0x297F4AA0)), unchecked((int)(0xE6ECFDDC)), unchecked((int)(0x8EEFD114)), unchecked(
			(int)(0x36EAA44C)), unchecked((int)(0x5EE98884)), unchecked((int)(0x430C380D)), 
			unchecked((int)(0x2B0F14C5)), unchecked((int)(0x930A619D)), unchecked((int)(0xFB094D55
			)), unchecked((int)(0xA8C1008F)), unchecked((int)(0xC0C22C47)), unchecked((int)(
			0x78C7591F)), unchecked((int)(0x10C475D7)), unchecked((int)(0x0D21C55E)), unchecked(
			(int)(0x6522E996)), unchecked((int)(0xDD279CCE)), unchecked((int)(0xB524B006)), 
			unchecked((int)(0x47EC84C7)), unchecked((int)(0x2FEFA80F)), unchecked((int)(0x97EADD57
			)), unchecked((int)(0xFFE9F19F)), unchecked((int)(0xE20C4116)), unchecked((int)(
			0x8A0F6DDE)), unchecked((int)(0x320A1886)), unchecked((int)(0x5A09344E)), unchecked(
			(int)(0x09C17994)), unchecked((int)(0x61C2555C)), unchecked((int)(0xD9C72004)), 
			unchecked((int)(0xB1C40CCC)), unchecked((int)(0xAC21BC45)), unchecked((int)(0xC422908D
			)), unchecked((int)(0x7C27E5D5)), unchecked((int)(0x1424C91D)), unchecked((int)(
			0xDBB77E61)), unchecked((int)(0xB3B452A9)), unchecked((int)(0x0BB127F1)), unchecked(
			(int)(0x63B20B39)), unchecked((int)(0x7E57BBB0)), unchecked((int)(0x16549778)), 
			unchecked((int)(0xAE51E220)), unchecked((int)(0xC652CEE8)), unchecked((int)(0x959A8332
			)), unchecked((int)(0xFD99AFFA)), unchecked((int)(0x459CDAA2)), unchecked((int)(
			0x2D9FF66A)), unchecked((int)(0x307A46E3)), unchecked((int)(0x58796A2B)), unchecked(
			(int)(0xE07C1F73)), unchecked((int)(0x887F33BB)), unchecked((int)(0xF56E0EF4)), 
			unchecked((int)(0x9D6D223C)), unchecked((int)(0x25685764)), unchecked((int)(0x4D6B7BAC
			)), unchecked((int)(0x508ECB25)), unchecked((int)(0x388DE7ED)), unchecked((int)(
			0x808892B5)), unchecked((int)(0xE88BBE7D)), unchecked((int)(0xBB43F3A7)), unchecked(
			(int)(0xD340DF6F)), unchecked((int)(0x6B45AA37)), unchecked((int)(0x034686FF)), 
			unchecked((int)(0x1EA33676)), unchecked((int)(0x76A01ABE)), unchecked((int)(0xCEA56FE6
			)), unchecked((int)(0xA6A6432E)), unchecked((int)(0x6935F452)), unchecked((int)(
			0x0136D89A)), unchecked((int)(0xB933ADC2)), unchecked((int)(0xD130810A)), unchecked(
			(int)(0xCCD53183)), unchecked((int)(0xA4D61D4B)), unchecked((int)(0x1CD36813)), 
			unchecked((int)(0x74D044DB)), unchecked((int)(0x27180901)), unchecked((int)(0x4F1B25C9
			)), unchecked((int)(0xF71E5091)), unchecked((int)(0x9F1D7C59)), unchecked((int)(
			0x82F8CCD0)), unchecked((int)(0xEAFBE018)), unchecked((int)(0x52FE9540)), unchecked(
			(int)(0x3AFDB988)), unchecked((int)(0xC8358D49)), unchecked((int)(0xA036A181)), 
			unchecked((int)(0x1833D4D9)), unchecked((int)(0x7030F811)), unchecked((int)(0x6DD54898
			)), unchecked((int)(0x05D66450)), unchecked((int)(0xBDD31108)), unchecked((int)(
			0xD5D03DC0)), unchecked((int)(0x8618701A)), unchecked((int)(0xEE1B5CD2)), unchecked(
			(int)(0x561E298A)), unchecked((int)(0x3E1D0542)), unchecked((int)(0x23F8B5CB)), 
			unchecked((int)(0x4BFB9903)), unchecked((int)(0xF3FEEC5B)), unchecked((int)(0x9BFDC093
			)), unchecked((int)(0x546E77EF)), unchecked((int)(0x3C6D5B27)), unchecked((int)(
			0x84682E7F)), unchecked((int)(0xEC6B02B7)), unchecked((int)(0xF18EB23E)), unchecked(
			(int)(0x998D9EF6)), unchecked((int)(0x2188EBAE)), unchecked((int)(0x498BC766)), 
			unchecked((int)(0x1A438ABC)), unchecked((int)(0x7240A674)), unchecked((int)(0xCA45D32C
			)), unchecked((int)(0xA246FFE4)), unchecked((int)(0xBFA34F6D)), unchecked((int)(
			0xD7A063A5)), unchecked((int)(0x6FA516FD)), unchecked((int)(0x07A63A35)), unchecked(
			(int)(0x8FD9098E)), unchecked((int)(0xE7DA2546)), unchecked((int)(0x5FDF501E)), 
			unchecked((int)(0x37DC7CD6)), unchecked((int)(0x2A39CC5F)), unchecked((int)(0x423AE097
			)), unchecked((int)(0xFA3F95CF)), unchecked((int)(0x923CB907)), unchecked((int)(
			0xC1F4F4DD)), unchecked((int)(0xA9F7D815)), unchecked((int)(0x11F2AD4D)), unchecked(
			(int)(0x79F18185)), unchecked((int)(0x6414310C)), unchecked((int)(0x0C171DC4)), 
			unchecked((int)(0xB412689C)), unchecked((int)(0xDC114454)), unchecked((int)(0x1382F328
			)), unchecked((int)(0x7B81DFE0)), unchecked((int)(0xC384AAB8)), unchecked((int)(
			0xAB878670)), unchecked((int)(0xB66236F9)), unchecked((int)(0xDE611A31)), unchecked(
			(int)(0x66646F69)), unchecked((int)(0x0E6743A1)), unchecked((int)(0x5DAF0E7B)), 
			unchecked((int)(0x35AC22B3)), unchecked((int)(0x8DA957EB)), unchecked((int)(0xE5AA7B23
			)), unchecked((int)(0xF84FCBAA)), unchecked((int)(0x904CE762)), unchecked((int)(
			0x2849923A)), unchecked((int)(0x404ABEF2)), unchecked((int)(0xB2828A33)), unchecked(
			(int)(0xDA81A6FB)), unchecked((int)(0x6284D3A3)), unchecked((int)(0x0A87FF6B)), 
			unchecked((int)(0x17624FE2)), unchecked((int)(0x7F61632A)), unchecked((int)(0xC7641672
			)), unchecked((int)(0xAF673ABA)), unchecked((int)(0xFCAF7760)), unchecked((int)(
			0x94AC5BA8)), unchecked((int)(0x2CA92EF0)), unchecked((int)(0x44AA0238)), unchecked(
			(int)(0x594FB2B1)), unchecked((int)(0x314C9E79)), unchecked((int)(0x8949EB21)), 
			unchecked((int)(0xE14AC7E9)), unchecked((int)(0x2ED97095)), unchecked((int)(0x46DA5C5D
			)), unchecked((int)(0xFEDF2905)), unchecked((int)(0x96DC05CD)), unchecked((int)(
			0x8B39B544)), unchecked((int)(0xE33A998C)), unchecked((int)(0x5B3FECD4)), unchecked(
			(int)(0x333CC01C)), unchecked((int)(0x60F48DC6)), unchecked((int)(0x08F7A10E)), 
			unchecked((int)(0xB0F2D456)), unchecked((int)(0xD8F1F89E)), unchecked((int)(0xC5144817
			)), unchecked((int)(0xAD1764DF)), unchecked((int)(0x15121187)), unchecked((int)(
			0x7D113D4F)), unchecked((int)(0x00000000)), unchecked((int)(0x493C7D27)), unchecked(
			(int)(0x9278FA4E)), unchecked((int)(0xDB448769)), unchecked((int)(0x211D826D)), 
			unchecked((int)(0x6821FF4A)), unchecked((int)(0xB3657823)), unchecked((int)(0xFA590504
			)), unchecked((int)(0x423B04DA)), unchecked((int)(0x0B0779FD)), unchecked((int)(
			0xD043FE94)), unchecked((int)(0x997F83B3)), unchecked((int)(0x632686B7)), unchecked(
			(int)(0x2A1AFB90)), unchecked((int)(0xF15E7CF9)), unchecked((int)(0xB86201DE)), 
			unchecked((int)(0x847609B4)), unchecked((int)(0xCD4A7493)), unchecked((int)(0x160EF3FA
			)), unchecked((int)(0x5F328EDD)), unchecked((int)(0xA56B8BD9)), unchecked((int)(
			0xEC57F6FE)), unchecked((int)(0x37137197)), unchecked((int)(0x7E2F0CB0)), unchecked(
			(int)(0xC64D0D6E)), unchecked((int)(0x8F717049)), unchecked((int)(0x5435F720)), 
			unchecked((int)(0x1D098A07)), unchecked((int)(0xE7508F03)), unchecked((int)(0xAE6CF224
			)), unchecked((int)(0x7528754D)), unchecked((int)(0x3C14086A)), unchecked((int)(
			0x0D006599)), unchecked((int)(0x443C18BE)), unchecked((int)(0x9F789FD7)), unchecked(
			(int)(0xD644E2F0)), unchecked((int)(0x2C1DE7F4)), unchecked((int)(0x65219AD3)), 
			unchecked((int)(0xBE651DBA)), unchecked((int)(0xF759609D)), unchecked((int)(0x4F3B6143
			)), unchecked((int)(0x06071C64)), unchecked((int)(0xDD439B0D)), unchecked((int)(
			0x947FE62A)), unchecked((int)(0x6E26E32E)), unchecked((int)(0x271A9E09)), unchecked(
			(int)(0xFC5E1960)), unchecked((int)(0xB5626447)), unchecked((int)(0x89766C2D)), 
			unchecked((int)(0xC04A110A)), unchecked((int)(0x1B0E9663)), unchecked((int)(0x5232EB44
			)), unchecked((int)(0xA86BEE40)), unchecked((int)(0xE1579367)), unchecked((int)(
			0x3A13140E)), unchecked((int)(0x732F6929)), unchecked((int)(0xCB4D68F7)), unchecked(
			(int)(0x827115D0)), unchecked((int)(0x593592B9)), unchecked((int)(0x1009EF9E)), 
			unchecked((int)(0xEA50EA9A)), unchecked((int)(0xA36C97BD)), unchecked((int)(0x782810D4
			)), unchecked((int)(0x31146DF3)), unchecked((int)(0x1A00CB32)), unchecked((int)(
			0x533CB615)), unchecked((int)(0x8878317C)), unchecked((int)(0xC1444C5B)), unchecked(
			(int)(0x3B1D495F)), unchecked((int)(0x72213478)), unchecked((int)(0xA965B311)), 
			unchecked((int)(0xE059CE36)), unchecked((int)(0x583BCFE8)), unchecked((int)(0x1107B2CF
			)), unchecked((int)(0xCA4335A6)), unchecked((int)(0x837F4881)), unchecked((int)(
			0x79264D85)), unchecked((int)(0x301A30A2)), unchecked((int)(0xEB5EB7CB)), unchecked(
			(int)(0xA262CAEC)), unchecked((int)(0x9E76C286)), unchecked((int)(0xD74ABFA1)), 
			unchecked((int)(0x0C0E38C8)), unchecked((int)(0x453245EF)), unchecked((int)(0xBF6B40EB
			)), unchecked((int)(0xF6573DCC)), unchecked((int)(0x2D13BAA5)), unchecked((int)(
			0x642FC782)), unchecked((int)(0xDC4DC65C)), unchecked((int)(0x9571BB7B)), unchecked(
			(int)(0x4E353C12)), unchecked((int)(0x07094135)), unchecked((int)(0xFD504431)), 
			unchecked((int)(0xB46C3916)), unchecked((int)(0x6F28BE7F)), unchecked((int)(0x2614C358
			)), unchecked((int)(0x1700AEAB)), unchecked((int)(0x5E3CD38C)), unchecked((int)(
			0x857854E5)), unchecked((int)(0xCC4429C2)), unchecked((int)(0x361D2CC6)), unchecked(
			(int)(0x7F2151E1)), unchecked((int)(0xA465D688)), unchecked((int)(0xED59ABAF)), 
			unchecked((int)(0x553BAA71)), unchecked((int)(0x1C07D756)), unchecked((int)(0xC743503F
			)), unchecked((int)(0x8E7F2D18)), unchecked((int)(0x7426281C)), unchecked((int)(
			0x3D1A553B)), unchecked((int)(0xE65ED252)), unchecked((int)(0xAF62AF75)), unchecked(
			(int)(0x9376A71F)), unchecked((int)(0xDA4ADA38)), unchecked((int)(0x010E5D51)), 
			unchecked((int)(0x48322076)), unchecked((int)(0xB26B2572)), unchecked((int)(0xFB575855
			)), unchecked((int)(0x2013DF3C)), unchecked((int)(0x692FA21B)), unchecked((int)(
			0xD14DA3C5)), unchecked((int)(0x9871DEE2)), unchecked((int)(0x4335598B)), unchecked(
			(int)(0x0A0924AC)), unchecked((int)(0xF05021A8)), unchecked((int)(0xB96C5C8F)), 
			unchecked((int)(0x6228DBE6)), unchecked((int)(0x2B14A6C1)), unchecked((int)(0x34019664
			)), unchecked((int)(0x7D3DEB43)), unchecked((int)(0xA6796C2A)), unchecked((int)(
			0xEF45110D)), unchecked((int)(0x151C1409)), unchecked((int)(0x5C20692E)), unchecked(
			(int)(0x8764EE47)), unchecked((int)(0xCE589360)), unchecked((int)(0x763A92BE)), 
			unchecked((int)(0x3F06EF99)), unchecked((int)(0xE44268F0)), unchecked((int)(0xAD7E15D7
			)), unchecked((int)(0x572710D3)), unchecked((int)(0x1E1B6DF4)), unchecked((int)(
			0xC55FEA9D)), unchecked((int)(0x8C6397BA)), unchecked((int)(0xB0779FD0)), unchecked(
			(int)(0xF94BE2F7)), unchecked((int)(0x220F659E)), unchecked((int)(0x6B3318B9)), 
			unchecked((int)(0x916A1DBD)), unchecked((int)(0xD856609A)), unchecked((int)(0x0312E7F3
			)), unchecked((int)(0x4A2E9AD4)), unchecked((int)(0xF24C9B0A)), unchecked((int)(
			0xBB70E62D)), unchecked((int)(0x60346144)), unchecked((int)(0x29081C63)), unchecked(
			(int)(0xD3511967)), unchecked((int)(0x9A6D6440)), unchecked((int)(0x4129E329)), 
			unchecked((int)(0x08159E0E)), unchecked((int)(0x3901F3FD)), unchecked((int)(0x703D8EDA
			)), unchecked((int)(0xAB7909B3)), unchecked((int)(0xE2457494)), unchecked((int)(
			0x181C7190)), unchecked((int)(0x51200CB7)), unchecked((int)(0x8A648BDE)), unchecked(
			(int)(0xC358F6F9)), unchecked((int)(0x7B3AF727)), unchecked((int)(0x32068A00)), 
			unchecked((int)(0xE9420D69)), unchecked((int)(0xA07E704E)), unchecked((int)(0x5A27754A
			)), unchecked((int)(0x131B086D)), unchecked((int)(0xC85F8F04)), unchecked((int)(
			0x8163F223)), unchecked((int)(0xBD77FA49)), unchecked((int)(0xF44B876E)), unchecked(
			(int)(0x2F0F0007)), unchecked((int)(0x66337D20)), unchecked((int)(0x9C6A7824)), 
			unchecked((int)(0xD5560503)), unchecked((int)(0x0E12826A)), unchecked((int)(0x472EFF4D
			)), unchecked((int)(0xFF4CFE93)), unchecked((int)(0xB67083B4)), unchecked((int)(
			0x6D3404DD)), unchecked((int)(0x240879FA)), unchecked((int)(0xDE517CFE)), unchecked(
			(int)(0x976D01D9)), unchecked((int)(0x4C2986B0)), unchecked((int)(0x0515FB97)), 
			unchecked((int)(0x2E015D56)), unchecked((int)(0x673D2071)), unchecked((int)(0xBC79A718
			)), unchecked((int)(0xF545DA3F)), unchecked((int)(0x0F1CDF3B)), unchecked((int)(
			0x4620A21C)), unchecked((int)(0x9D642575)), unchecked((int)(0xD4585852)), unchecked(
			(int)(0x6C3A598C)), unchecked((int)(0x250624AB)), unchecked((int)(0xFE42A3C2)), 
			unchecked((int)(0xB77EDEE5)), unchecked((int)(0x4D27DBE1)), unchecked((int)(0x041BA6C6
			)), unchecked((int)(0xDF5F21AF)), unchecked((int)(0x96635C88)), unchecked((int)(
			0xAA7754E2)), unchecked((int)(0xE34B29C5)), unchecked((int)(0x380FAEAC)), unchecked(
			(int)(0x7133D38B)), unchecked((int)(0x8B6AD68F)), unchecked((int)(0xC256ABA8)), 
			unchecked((int)(0x19122CC1)), unchecked((int)(0x502E51E6)), unchecked((int)(0xE84C5038
			)), unchecked((int)(0xA1702D1F)), unchecked((int)(0x7A34AA76)), unchecked((int)(
			0x3308D751)), unchecked((int)(0xC951D255)), unchecked((int)(0x806DAF72)), unchecked(
			(int)(0x5B29281B)), unchecked((int)(0x1215553C)), unchecked((int)(0x230138CF)), 
			unchecked((int)(0x6A3D45E8)), unchecked((int)(0xB179C281)), unchecked((int)(0xF845BFA6
			)), unchecked((int)(0x021CBAA2)), unchecked((int)(0x4B20C785)), unchecked((int)(
			0x906440EC)), unchecked((int)(0xD9583DCB)), unchecked((int)(0x613A3C15)), unchecked(
			(int)(0x28064132)), unchecked((int)(0xF342C65B)), unchecked((int)(0xBA7EBB7C)), 
			unchecked((int)(0x4027BE78)), unchecked((int)(0x091BC35F)), unchecked((int)(0xD25F4436
			)), unchecked((int)(0x9B633911)), unchecked((int)(0xA777317B)), unchecked((int)(
			0xEE4B4C5C)), unchecked((int)(0x350FCB35)), unchecked((int)(0x7C33B612)), unchecked(
			(int)(0x866AB316)), unchecked((int)(0xCF56CE31)), unchecked((int)(0x14124958)), 
			unchecked((int)(0x5D2E347F)), unchecked((int)(0xE54C35A1)), unchecked((int)(0xAC704886
			)), unchecked((int)(0x7734CFEF)), unchecked((int)(0x3E08B2C8)), unchecked((int)(
			0xC451B7CC)), unchecked((int)(0x8D6DCAEB)), unchecked((int)(0x56294D82)), unchecked(
			(int)(0x1F1530A5)) };
		// CRC polynomial tables generated by:
		// java -cp build/test/classes/:build/classes/ \
		//   org.apache.hadoop.util.TestPureJavaCrc32\$Table 82F63B78
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
