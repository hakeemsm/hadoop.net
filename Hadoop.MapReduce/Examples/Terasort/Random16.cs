using Sharpen;

namespace Org.Apache.Hadoop.Examples.Terasort
{
	/// <summary>This class implements a 128-bit linear congruential generator.</summary>
	/// <remarks>
	/// This class implements a 128-bit linear congruential generator.
	/// Specifically, if X0 is the most recently issued 128-bit random
	/// number (or a seed of 0 if no random number has already been generated,
	/// the next number to be generated, X1, is equal to:
	/// X1 = (a * X0 + c) mod 2**128
	/// where a is 47026247687942121848144207491837523525
	/// or 0x2360ed051fc65da44385df649fccf645
	/// and c is 98910279301475397889117759788405497857
	/// or 0x4a696d47726179524950202020202001
	/// The coefficient "a" is suggested by:
	/// Pierre L'Ecuyer, "Tables of linear congruential generators of different
	/// sizes and good lattice structure", Mathematics of Computation, 68
	/// pp. 249 - 260 (1999)
	/// http://www.ams.org/mcom/1999-68-225/S0025-5718-99-00996-5/S0025-5718-99-00996-5.pdf
	/// The constant "c" meets the simple suggestion by the same reference that
	/// it be odd.
	/// There is also a facility for quickly advancing the state of the
	/// generator by a fixed number of steps - this facilitates parallel
	/// generation.
	/// This is based on 1.0 of rand16.c from Chris Nyberg
	/// <chris.nyberg@ordinal.com>.
	/// </remarks>
	internal class Random16
	{
		/// <summary>The "Gen" array contain powers of 2 of the linear congruential generator.
		/// 	</summary>
		/// <remarks>
		/// The "Gen" array contain powers of 2 of the linear congruential generator.
		/// The index 0 struct contain the "a" coefficient and "c" constant for the
		/// generator.  That is, the generator is:
		/// f(x) = (Gen[0].a * x + Gen[0].c) mod 2**128
		/// All structs after the first contain an "a" and "c" that
		/// comprise the square of the previous function.
		/// f**2(x) = (Gen[1].a * x + Gen[1].c) mod 2**128
		/// f**4(x) = (Gen[2].a * x + Gen[2].c) mod 2**128
		/// f**8(x) = (Gen[3].a * x + Gen[3].c) mod 2**128
		/// ...
		/// </remarks>
		private class RandomConstant
		{
			internal readonly Unsigned16 a;

			internal readonly Unsigned16 c;

			public RandomConstant(string left, string right)
			{
				a = new Unsigned16(left);
				c = new Unsigned16(right);
			}
		}

		private static readonly Random16.RandomConstant[] genArray = new Random16.RandomConstant
			[] { new Random16.RandomConstant("2360ed051fc65da44385df649fccf645", "4a696d47726179524950202020202001"
			), new Random16.RandomConstant("17bce35bdf69743c529ed9eb20e0ae99", "95e0e48262b3edfe04479485c755b646"
			), new Random16.RandomConstant("f4dd417327db7a9bd194dfbe42d45771", "882a02c315362b60765f100068b33a1c"
			), new Random16.RandomConstant("6347af777a7898f6d1a2d6f33505ffe1", "5efc4abfaca23e8ca8edb1f2dfbf6478"
			), new Random16.RandomConstant("b6a4239f3b315f84f6ef6d3d288c03c1", "f25bd15439d16af594c1b1bafa6239f0"
			), new Random16.RandomConstant("2c82901ad1cb0cd182b631ba6b261781", "89ca67c29c9397d59c612596145db7e0"
			), new Random16.RandomConstant("dab03f988288676ee49e66c4d2746f01", "8b6ae036713bd578a8093c8eae5c7fc0"
			), new Random16.RandomConstant("602167331d86cf5684fe009a6d09de01", "98a2542fd23d0dbdff3b886cdb1d3f80"
			), new Random16.RandomConstant("61ecb5c24d95b058f04c80a23697bc01", "954db923fdb7933e947cd1edcecb7f00"
			), new Random16.RandomConstant("4a5c31e0654c28aa60474e83bf3f7801", "00be4a36657c98cd204e8c8af7dafe00"
			), new Random16.RandomConstant("ae4f079d54fbece1478331d3c6bef001", "991965329dccb28d581199ab18c5fc00"
			), new Random16.RandomConstant("101b8cb830c7cb927ff1ed50ae7de001", "e1a8705b63ad5b8cd6c3d268d5cbf800"
			), new Random16.RandomConstant("f54a27fc056b00e7563f3505e0fbc001", "2b657bbfd6ed9d632079e70c3c97f000"
			), new Random16.RandomConstant("df8a6fc1a833d201f98d719dd1f78001", "59b60ee4c52fa49e9fe90682bd2fe000"
			), new Random16.RandomConstant("5480a5015f101a4ea7e3f183e3ef0001", "cc099c88030679464fe86aae8a5fc000"
			), new Random16.RandomConstant("a498509e76e5d7925f539c28c7de0001", "06b9abff9f9f33dd30362c0154bf8000"
			), new Random16.RandomConstant("0798a3d8b10dc72e60121cd58fbc0001", "e296707121688d5a0260b293a97f0000"
			), new Random16.RandomConstant("1647d1e78ec02e665fafcbbb1f780001", "189ffc4701ff23cb8f8acf6b52fe0000"
			), new Random16.RandomConstant("a7c982285e72bf8c0c8ddfb63ef00001", "5141110ab208fb9d61fb47e6a5fc0000"
			), new Random16.RandomConstant("3eb78ee8fb8c56dbc5d4e06c7de00001", "3c97caa62540f2948d8d340d4bf80000"
			), new Random16.RandomConstant("72d03b6f4681f2f9fe8e44d8fbc00001", "1b25cb9cfe5a0c963174f91a97f00000"
			), new Random16.RandomConstant("ea85f81e4f502c9bc8ae99b1f7800001", "0c644570b4a487103c5436352fe00000"
			), new Random16.RandomConstant("629c320db08b00c6bfa57363ef000001", "3d0589c28869472bde517c6a5fc00000"
			), new Random16.RandomConstant("c5c4b9ce268d074a386be6c7de000001", "bc95e5ab36477e65534738d4bf800000"
			), new Random16.RandomConstant("f30bbbbed1596187555bcd8fbc000001", "ddb02ff72a031c01011f71a97f000000"
			), new Random16.RandomConstant("4a1000fb26c9eeda3cc79b1f78000001", "2561426086d9acdb6c82e352fe000000"
			), new Random16.RandomConstant("89fb5307f6bf8ce2c1cf363ef0000001", "64a788e3c118ed1c8215c6a5fc000000"
			), new Random16.RandomConstant("830b7b3358a5d67ea49e6c7de0000001", "e65ea321908627cfa86b8d4bf8000000"
			), new Random16.RandomConstant("fd8a51da91a69fe1cd3cd8fbc0000001", "53d27225604d85f9e1d71a97f0000000"
			), new Random16.RandomConstant("901a48b642b90b55aa79b1f780000001", "ca5ec7a3ed1fe55e07ae352fe0000000"
			), new Random16.RandomConstant("118cdefdf32144f394f363ef00000001", "4daebb2e085330651f5c6a5fc0000000"
			), new Random16.RandomConstant("0a88c0a91cff430829e6c7de00000001", "9d6f1a00a8f3f76e7eb8d4bf80000000"
			), new Random16.RandomConstant("433bef4314f16a9453cd8fbc00000001", "158c62f2b31e496dfd71a97f00000000"
			), new Random16.RandomConstant("c294b02995ae6738a79b1f7800000001", "290e84a2eb15fd1ffae352fe00000000"
			), new Random16.RandomConstant("913575e0da8b16b14f363ef000000001", "e3dc1bfbe991a34ff5c6a5fc00000000"
			), new Random16.RandomConstant("2f61b9f871cf4e629e6c7de000000001", "ddf540d020b9eadfeb8d4bf800000000"
			), new Random16.RandomConstant("78d26ccbd68320c53cd8fbc000000001", "8ee4950177ce66bfd71a97f000000000"
			), new Random16.RandomConstant("8b7ebd037898518a79b1f78000000001", "39e0f787c907117fae352fe000000000"
			), new Random16.RandomConstant("0b5507b61f78e314f363ef0000000001", "659d2522f7b732ff5c6a5fc000000000"
			), new Random16.RandomConstant("4f884628f812c629e6c7de0000000001", "9e8722938612a5feb8d4bf8000000000"
			), new Random16.RandomConstant("be896744d4a98c53cd8fbc0000000001", "e941a65d66b64bfd71a97f0000000000"
			), new Random16.RandomConstant("daf63a553b6318a79b1f780000000001", "7b50d19437b097fae352fe0000000000"
			), new Random16.RandomConstant("2d7a23d8bf06314f363ef00000000001", "59d7b68e18712ff5c6a5fc0000000000"
			), new Random16.RandomConstant("392b046a9f0c629e6c7de00000000001", "4087bab2d5225feb8d4bf80000000000"
			), new Random16.RandomConstant("eb30fbb9c218c53cd8fbc00000000001", "b470abc03b44bfd71a97f00000000000"
			), new Random16.RandomConstant("b9cdc30594318a79b1f7800000000001", "366630eaba897fae352fe00000000000"
			), new Random16.RandomConstant("014ab453686314f363ef000000000001", "a2dfc77e8512ff5c6a5fc00000000000"
			), new Random16.RandomConstant("395221c7d0c629e6c7de000000000001", "1e0d25a14a25feb8d4bf800000000000"
			), new Random16.RandomConstant("4d972813a18c53cd8fbc000000000001", "9d50a5d3944bfd71a97f000000000000"
			), new Random16.RandomConstant("06f9e2374318a79b1f78000000000001", "bf7ab5eb2897fae352fe000000000000"
			), new Random16.RandomConstant("bd220cae86314f363ef0000000000001", "925b14e6512ff5c6a5fc000000000000"
			), new Random16.RandomConstant("36fd3a5d0c629e6c7de0000000000001", "724cce0ca25feb8d4bf8000000000000"
			), new Random16.RandomConstant("60def8ba18c53cd8fbc0000000000001", "1af42d1944bfd71a97f0000000000000"
			), new Random16.RandomConstant("8d500174318a79b1f780000000000001", "0f529e32897fae352fe0000000000000"
			), new Random16.RandomConstant("48e842e86314f363ef00000000000001", "844e4c6512ff5c6a5fc0000000000000"
			), new Random16.RandomConstant("4af185d0c629e6c7de00000000000001", "9f40d8ca25feb8d4bf80000000000000"
			), new Random16.RandomConstant("7a670ba18c53cd8fbc00000000000001", "9912b1944bfd71a97f00000000000000"
			), new Random16.RandomConstant("86de174318a79b1f7800000000000001", "9c69632897fae352fe00000000000000"
			), new Random16.RandomConstant("55fc2e86314f363ef000000000000001", "e1e2c6512ff5c6a5fc00000000000000"
			), new Random16.RandomConstant("ccf85d0c629e6c7de000000000000001", "68058ca25feb8d4bf800000000000000"
			), new Random16.RandomConstant("1df0ba18c53cd8fbc000000000000001", "610b1944bfd71a97f000000000000000"
			), new Random16.RandomConstant("4be174318a79b1f78000000000000001", "061632897fae352fe000000000000000"
			), new Random16.RandomConstant("d7c2e86314f363ef0000000000000001", "1c2c6512ff5c6a5fc000000000000000"
			), new Random16.RandomConstant("af85d0c629e6c7de0000000000000001", "7858ca25feb8d4bf8000000000000000"
			), new Random16.RandomConstant("5f0ba18c53cd8fbc0000000000000001", "f0b1944bfd71a97f0000000000000000"
			), new Random16.RandomConstant("be174318a79b1f780000000000000001", "e1632897fae352fe0000000000000000"
			), new Random16.RandomConstant("7c2e86314f363ef00000000000000001", "c2c6512ff5c6a5fc0000000000000000"
			), new Random16.RandomConstant("f85d0c629e6c7de00000000000000001", "858ca25feb8d4bf80000000000000000"
			), new Random16.RandomConstant("f0ba18c53cd8fbc00000000000000001", "0b1944bfd71a97f00000000000000000"
			), new Random16.RandomConstant("e174318a79b1f7800000000000000001", "1632897fae352fe00000000000000000"
			), new Random16.RandomConstant("c2e86314f363ef000000000000000001", "2c6512ff5c6a5fc00000000000000000"
			), new Random16.RandomConstant("85d0c629e6c7de000000000000000001", "58ca25feb8d4bf800000000000000000"
			), new Random16.RandomConstant("0ba18c53cd8fbc000000000000000001", "b1944bfd71a97f000000000000000000"
			), new Random16.RandomConstant("174318a79b1f78000000000000000001", "632897fae352fe000000000000000000"
			), new Random16.RandomConstant("2e86314f363ef0000000000000000001", "c6512ff5c6a5fc000000000000000000"
			), new Random16.RandomConstant("5d0c629e6c7de0000000000000000001", "8ca25feb8d4bf8000000000000000000"
			), new Random16.RandomConstant("ba18c53cd8fbc0000000000000000001", "1944bfd71a97f0000000000000000000"
			), new Random16.RandomConstant("74318a79b1f780000000000000000001", "32897fae352fe0000000000000000000"
			), new Random16.RandomConstant("e86314f363ef00000000000000000001", "6512ff5c6a5fc0000000000000000000"
			), new Random16.RandomConstant("d0c629e6c7de00000000000000000001", "ca25feb8d4bf80000000000000000000"
			), new Random16.RandomConstant("a18c53cd8fbc00000000000000000001", "944bfd71a97f00000000000000000000"
			), new Random16.RandomConstant("4318a79b1f7800000000000000000001", "2897fae352fe00000000000000000000"
			), new Random16.RandomConstant("86314f363ef000000000000000000001", "512ff5c6a5fc00000000000000000000"
			), new Random16.RandomConstant("0c629e6c7de000000000000000000001", "a25feb8d4bf800000000000000000000"
			), new Random16.RandomConstant("18c53cd8fbc000000000000000000001", "44bfd71a97f000000000000000000000"
			), new Random16.RandomConstant("318a79b1f78000000000000000000001", "897fae352fe000000000000000000000"
			), new Random16.RandomConstant("6314f363ef0000000000000000000001", "12ff5c6a5fc000000000000000000000"
			), new Random16.RandomConstant("c629e6c7de0000000000000000000001", "25feb8d4bf8000000000000000000000"
			), new Random16.RandomConstant("8c53cd8fbc0000000000000000000001", "4bfd71a97f0000000000000000000000"
			), new Random16.RandomConstant("18a79b1f780000000000000000000001", "97fae352fe0000000000000000000000"
			), new Random16.RandomConstant("314f363ef00000000000000000000001", "2ff5c6a5fc0000000000000000000000"
			), new Random16.RandomConstant("629e6c7de00000000000000000000001", "5feb8d4bf80000000000000000000000"
			), new Random16.RandomConstant("c53cd8fbc00000000000000000000001", "bfd71a97f00000000000000000000000"
			), new Random16.RandomConstant("8a79b1f7800000000000000000000001", "7fae352fe00000000000000000000000"
			), new Random16.RandomConstant("14f363ef000000000000000000000001", "ff5c6a5fc00000000000000000000000"
			), new Random16.RandomConstant("29e6c7de000000000000000000000001", "feb8d4bf800000000000000000000000"
			), new Random16.RandomConstant("53cd8fbc000000000000000000000001", "fd71a97f000000000000000000000000"
			), new Random16.RandomConstant("a79b1f78000000000000000000000001", "fae352fe000000000000000000000000"
			), new Random16.RandomConstant("4f363ef0000000000000000000000001", "f5c6a5fc000000000000000000000000"
			), new Random16.RandomConstant("9e6c7de0000000000000000000000001", "eb8d4bf8000000000000000000000000"
			), new Random16.RandomConstant("3cd8fbc0000000000000000000000001", "d71a97f0000000000000000000000000"
			), new Random16.RandomConstant("79b1f780000000000000000000000001", "ae352fe0000000000000000000000000"
			), new Random16.RandomConstant("f363ef00000000000000000000000001", "5c6a5fc0000000000000000000000000"
			), new Random16.RandomConstant("e6c7de00000000000000000000000001", "b8d4bf80000000000000000000000000"
			), new Random16.RandomConstant("cd8fbc00000000000000000000000001", "71a97f00000000000000000000000000"
			), new Random16.RandomConstant("9b1f7800000000000000000000000001", "e352fe00000000000000000000000000"
			), new Random16.RandomConstant("363ef000000000000000000000000001", "c6a5fc00000000000000000000000000"
			), new Random16.RandomConstant("6c7de000000000000000000000000001", "8d4bf800000000000000000000000000"
			), new Random16.RandomConstant("d8fbc000000000000000000000000001", "1a97f000000000000000000000000000"
			), new Random16.RandomConstant("b1f78000000000000000000000000001", "352fe000000000000000000000000000"
			), new Random16.RandomConstant("63ef0000000000000000000000000001", "6a5fc000000000000000000000000000"
			), new Random16.RandomConstant("c7de0000000000000000000000000001", "d4bf8000000000000000000000000000"
			), new Random16.RandomConstant("8fbc0000000000000000000000000001", "a97f0000000000000000000000000000"
			), new Random16.RandomConstant("1f780000000000000000000000000001", "52fe0000000000000000000000000000"
			), new Random16.RandomConstant("3ef00000000000000000000000000001", "a5fc0000000000000000000000000000"
			), new Random16.RandomConstant("7de00000000000000000000000000001", "4bf80000000000000000000000000000"
			), new Random16.RandomConstant("fbc00000000000000000000000000001", "97f00000000000000000000000000000"
			), new Random16.RandomConstant("f7800000000000000000000000000001", "2fe00000000000000000000000000000"
			), new Random16.RandomConstant("ef000000000000000000000000000001", "5fc00000000000000000000000000000"
			), new Random16.RandomConstant("de000000000000000000000000000001", "bf800000000000000000000000000000"
			), new Random16.RandomConstant("bc000000000000000000000000000001", "7f000000000000000000000000000000"
			), new Random16.RandomConstant("78000000000000000000000000000001", "fe000000000000000000000000000000"
			), new Random16.RandomConstant("f0000000000000000000000000000001", "fc000000000000000000000000000000"
			), new Random16.RandomConstant("e0000000000000000000000000000001", "f8000000000000000000000000000000"
			), new Random16.RandomConstant("c0000000000000000000000000000001", "f0000000000000000000000000000000"
			), new Random16.RandomConstant("80000000000000000000000000000001", "e0000000000000000000000000000000"
			), new Random16.RandomConstant("00000000000000000000000000000001", "c0000000000000000000000000000000"
			), new Random16.RandomConstant("00000000000000000000000000000001", "80000000000000000000000000000000"
			) };

		/* [  0] */
		/* [  1] */
		/* [  2] */
		/* [  3] */
		/* [  4] */
		/* [  5] */
		/* [  6] */
		/* [  7] */
		/* [  8] */
		/* [  9] */
		/* [ 10] */
		/* [ 11] */
		/* [ 12] */
		/* [ 13] */
		/* [ 14] */
		/* [ 15] */
		/* [ 16] */
		/* [ 17] */
		/* [ 18] */
		/* [ 19] */
		/* [ 20] */
		/* [ 21] */
		/* [ 22] */
		/* [ 23] */
		/* [ 24] */
		/* [ 25] */
		/* [ 26] */
		/* [ 27] */
		/* [ 28] */
		/* [ 29] */
		/* [ 30] */
		/* [ 31] */
		/* [ 32] */
		/* [ 33] */
		/* [ 34] */
		/* [ 35] */
		/* [ 36] */
		/* [ 37] */
		/* [ 38] */
		/* [ 39] */
		/* [ 40] */
		/* [ 41] */
		/* [ 42] */
		/* [ 43] */
		/* [ 44] */
		/* [ 45] */
		/* [ 46] */
		/* [ 47] */
		/* [ 48] */
		/* [ 49] */
		/* [ 50] */
		/* [ 51] */
		/* [ 52] */
		/* [ 53] */
		/* [ 54] */
		/* [ 55] */
		/* [ 56] */
		/* [ 57] */
		/* [ 58] */
		/* [ 59] */
		/* [ 60] */
		/* [ 61] */
		/* [ 62] */
		/* [ 63] */
		/* [ 64] */
		/* [ 65] */
		/* [ 66] */
		/* [ 67] */
		/* [ 68] */
		/* [ 69] */
		/* [ 70] */
		/* [ 71] */
		/* [ 72] */
		/* [ 73] */
		/* [ 74] */
		/* [ 75] */
		/* [ 76] */
		/* [ 77] */
		/* [ 78] */
		/* [ 79] */
		/* [ 80] */
		/* [ 81] */
		/* [ 82] */
		/* [ 83] */
		/* [ 84] */
		/* [ 85] */
		/* [ 86] */
		/* [ 87] */
		/* [ 88] */
		/* [ 89] */
		/* [ 90] */
		/* [ 91] */
		/* [ 92] */
		/* [ 93] */
		/* [ 94] */
		/* [ 95] */
		/* [ 96] */
		/* [ 97] */
		/* [ 98] */
		/* [ 99] */
		/* [100] */
		/* [101] */
		/* [102] */
		/* [103] */
		/* [104] */
		/* [105] */
		/* [106] */
		/* [107] */
		/* [108] */
		/* [109] */
		/* [110] */
		/* [111] */
		/* [112] */
		/* [113] */
		/* [114] */
		/* [115] */
		/* [116] */
		/* [117] */
		/* [118] */
		/* [119] */
		/* [120] */
		/* [121] */
		/* [122] */
		/* [123] */
		/* [124] */
		/* [125] */
		/* [126] */
		/* [127] */
		/// <summary>
		/// generate the random number that is "advance" steps
		/// from an initial random number of 0.
		/// </summary>
		/// <remarks>
		/// generate the random number that is "advance" steps
		/// from an initial random number of 0.  This is done by
		/// starting with 0, and then advancing the by the
		/// appropriate powers of 2 of the linear congruential
		/// generator.
		/// </remarks>
		public static Unsigned16 SkipAhead(Unsigned16 advance)
		{
			Unsigned16 result = new Unsigned16();
			long bit_map;
			bit_map = advance.GetLow8();
			for (int i = 0; bit_map != 0 && i < 64; i++)
			{
				if ((bit_map & (1L << i)) != 0)
				{
					/* advance random number by f**(2**i) (x)
					*/
					result.Multiply(genArray[i].a);
					result.Add(genArray[i].c);
					bit_map &= ~(1L << i);
				}
			}
			bit_map = advance.GetHigh8();
			for (int i_1 = 0; bit_map != 0 && i_1 < 64; i_1++)
			{
				if ((bit_map & (1L << i_1)) != 0)
				{
					/* advance random number by f**(2**(i + 64)) (x)
					*/
					result.Multiply(genArray[i_1 + 64].a);
					result.Add(genArray[i_1 + 64].c);
					bit_map &= ~(1L << i_1);
				}
			}
			return result;
		}

		/// <summary>Generate the next 16 byte random number.</summary>
		public static void NextRand(Unsigned16 rand)
		{
			/* advance the random number forward once using the linear congruential
			* generator, and then return the new random number
			*/
			rand.Multiply(genArray[0].a);
			rand.Add(genArray[0].c);
		}
	}
}
