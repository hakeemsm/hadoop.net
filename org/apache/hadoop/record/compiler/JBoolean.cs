using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class JBoolean : org.apache.hadoop.record.compiler.JType
	{
		internal class JavaBoolean : org.apache.hadoop.record.compiler.JType.JavaType
		{
			internal JavaBoolean(JBoolean _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override void genCompareTo(org.apache.hadoop.record.compiler.CodeBuffer 
				cb, string fname, string other)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = (" + fname
					 + " == " + other + ")? 0 : (" + fname + "?1:-1);\n");
			}

			internal override string getTypeIDObjectString()
			{
				return "org.apache.hadoop.record.meta.TypeID.BoolTypeID";
			}

			internal override void genHashCode(org.apache.hadoop.record.compiler.CodeBuffer cb
				, string fname)
			{
				cb.append(org.apache.hadoop.record.compiler.Consts.RIO_PREFIX + "ret = (" + fname
					 + ")?0:1;\n");
			}

			// In Binary format, boolean is written as byte. true = 1, false = 0
			internal override void genSlurpBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb, string b, string s, string l)
			{
				cb.append("{\n");
				cb.append("if (" + l + "<1) {\n");
				cb.append("throw new java.io.IOException(\"Boolean is exactly 1 byte." + " Provided buffer is smaller.\");\n"
					);
				cb.append("}\n");
				cb.append(s + "++; " + l + "--;\n");
				cb.append("}\n");
			}

			// In Binary format, boolean is written as byte. true = 1, false = 0
			internal override void genCompareBytes(org.apache.hadoop.record.compiler.CodeBuffer
				 cb)
			{
				cb.append("{\n");
				cb.append("if (l1<1 || l2<1) {\n");
				cb.append("throw new java.io.IOException(\"Boolean is exactly 1 byte." + " Provided buffer is smaller.\");\n"
					);
				cb.append("}\n");
				cb.append("if (b1[s1] != b2[s2]) {\n");
				cb.append("return (b1[s1]<b2[s2])? -1 : 0;\n");
				cb.append("}\n");
				cb.append("s1++; s2++; l1--; l2--;\n");
				cb.append("}\n");
			}

			private readonly JBoolean _enclosing;
		}

		internal class CppBoolean : org.apache.hadoop.record.compiler.JType.CppType
		{
			internal CppBoolean(JBoolean _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override string getTypeIDObjectString()
			{
				return "new ::hadoop::TypeID(::hadoop::RIOTYPE_BOOL)";
			}

			private readonly JBoolean _enclosing;
		}

		/// <summary>Creates a new instance of JBoolean</summary>
		public JBoolean()
		{
			setJavaType(new org.apache.hadoop.record.compiler.JBoolean.JavaBoolean(this));
			setCppType(new org.apache.hadoop.record.compiler.JBoolean.CppBoolean(this));
			setCType(new org.apache.hadoop.record.compiler.JType.CType(this));
		}

		internal override string getSignature()
		{
			return "z";
		}
	}
}
