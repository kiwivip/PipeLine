#!/usr/bin/perl
# ==============================================================================
# function:     双子星项目的配置及子程式文件
# create date:  2011.12.14
# author：	kiwi
# ==============================================================================

package Mod_Pipe;
use 5.12.4;					# Ubuntu11.10默认perl版本
no strict "refs";
use constant pi => (4 * atan2(1,1));
use constant e => 2.718281828459;
use utf8;
use Encode;
use Encode::Guess;
use Encode::HanConvert;				
use Math::Polygon;
use Data::Dumper::Simple;

# ------------------------------------------------------------------------------

our $index_similarity = 0.88;	# define the index of similarity，约束聚类程度
our $step = 0.0025;

# ------------------------------------------------------------------------------
# define:	config the connection to mysql
# ------------------------------------------------------------------------------
our $database = "d01";
our $host = "192.168.1.15";
our $username = "data";		
our $password = "skst";			

# ------------------------------------------------------------------------------
# define:	filepath of the data on hand
# ------------------------------------------------------------------------------
our $dir = "/home/lifeix/桌面/PipeLine_双子星/";	# 项目路径

our $file_pois = $dir."townfile.csv";		# 数据库原所有的POI文本文件

our $dir_map = $dir."Map/";			# 爬虫的输出文件路径
our $dir_clean = $dir."Clean/";			# 清洗的输出文件路径
our $dir_cluster = $dir."Cluster/";		# 聚类组输出文件路径
our $dir_bad = $dir."Bad/";			# 不合法POI名称文件路径
our $dir_dup = $dir."Dup/";			# 重复POI_ID组log文件

our $file_centerll = $dir."ll_in_china";	# 这个暂时不用

# ------------------------------------------------------------------------------
# function:	哈希表areacode存储邮政编码映射关系，这个是为了标准化电话号码，暂时不用
# ------------------------------------------------------------------------------
my %areacode;
my $file_areacode = $dir."./areacode";
open AREA , "<" , $file_areacode or die "$file_areacode:$!";
foreach(<AREA>){
    chomp;
    my ($city,$code) = split " " , Decode2utf8($_) ;
    $code =~ s/\s*$//i;
    $areacode{$city} = $code;
    $city =~ s/市$//i;
    $areacode{$city} = $code;
}
#TestHash(\%areacode);

# ------------------------------------------------------------------------------
my %num2month = ( '01' => "Jan" , '02' => "Feb" , '03' => "Mar" , '04' => "Apr" ,
		 '05' => "May" , '06' => "Jun" , '07' => "Jul" , '08' => "Aug" ,
		 '09' => "Sep" , '10' => "Oct" , '11' => "Nov" , '12' => "Dec");
my %month2num = reverse %num2month;
my %H2W = ("a"=>"ａ","b"=>"ｂ","c"=>"ｃ","d"=>"ｄ","e"=>"ｅ","f"=>"ｆ","g"=>"ｇ",
"h"=>"ｈ","i"=>"ｉ","j"=>"ｊ","k"=>"ｋ","l"=>"ｌ","m"=>"ｍ","n"=>"ｎ","o"=>"ｏ",
"p"=>"ｐ","q"=>"ｑ","r"=>"ｒ","s"=>"ｓ","t"=>"ｔ","u"=>"ｕ","v"=>"ｖ","w"=>"ｗ",
"x"=>"ｘ","y"=>"ｙ","z"=>"ｚ","A"=>"Ａ","B"=>"Ｂ","C"=>"Ｃ","D"=>"Ｄ","E"=>"Ｅ",
"F"=>"Ｆ","G"=>"Ｇ","H"=>"Ｈ","I"=>"Ｉ","J"=>"Ｊ","K"=>"Ｋ","L"=>"Ｌ","M"=>"Ｍ",
"N"=>"Ｎ","O"=>"Ｏ","P"=>"Ｐ","Q"=>"Ｑ","R"=>"Ｒ","S"=>"Ｓ","T"=>"Ｔ","U"=>"Ｕ",
"V"=>"Ｖ","W"=>"Ｗ","X"=>"Ｘ","Y"=>"Ｙ","Z"=>"Ｚ","/0]"=>"０","1"=>"１","2"=>"２",
"3"=>"３","4"=>"４","5"=>"５","6"=>"６","7"=>"７","8"=>"８","9"=>"９","\`"=>"\｀",
"\~"=>"\～","\!"=>"\！","\@"=>"\＠","\#"=>"\＃"," \$"=>"＄","\%"=>"\％","^"=>"＾",
"\&"=>"\＆","*"=>"＊","("=>"（",")"=>"）","-"=>"－","_"=>"＿","="=>"＝","+"=>"＋",
"\\"=>"\＼","|"=>"\｜","\["=>"\［","\]"=>"\］","{"=>"\｛","}"=>"\｝",";"=>"；",
":"=>"：","'"=>"＇","\""=>"\＂","\,"=>"，","\<"=>"＜","."=>"。","\>"=>"＞",
"\/"=>"\／","\?"=>"？"," "=>"　");
my %W2H = reverse %H2W;

# =========================== functions ========================================

# ------------------------------------------------------------------------------
# function:	Return the Levenshtein distance (also called Edit distance)
# 		between two strings
#
# The Levenshtein distance (LD) is a measure of similarity between two
# strings, denoted here by s1 and s2. The distance is the number of
# deletions, insertions or substitutions required to transform s1 into
# s2. The greater the distance, the more different the strings are.
#
# The algorithm employs a proximity matrix, which denotes the distances
# between substrings of the two given strings. Read the embedded comments
# for more info. If you want a deep understanding of the algorithm, print
# the matrix for some test strings and study it
#
# The beauty of this system is that nothing is magical -
# 	the distance is intuitively understandable by humans
#
# The distance is named after the Russian scientist Vladimir Levenshtein,
# 	who devised the algorithm in 1965
# ------------------------------------------------------------------------------
sub levenshtein
{
    # $s1 and $s2 are the two strings
    # 	$len1 and $len2 are their respective lengths
    my ($s1, $s2) = @_;
    my ($len1, $len2) = (length $s1, length $s2);

    # If one of the strings is empty,
    # 	the distance is the length of the other string
    return $len2 if ($len1 == 0);
    return $len1 if ($len2 == 0);

    my %mat;

    # Init the distance matrix
    #
    # The first row to 0..$len1
    # The first column to 0..$len2
    # The rest to 0
    #
    # The first row and column are initialized so to denote distance from the empty string
    #
    for (my $i = 0; $i <= $len1; ++$i)
    {
        for (my $j = 0; $j <= $len2; ++$j)
        {
            $mat{$i}{$j} = 0;
            $mat{0}{$j} = $j;
        }

        $mat{$i}{0} = $i;
    }

    # Some char-by-char processing is ahead, so prepare array of chars from the strings
    #
    my @ar1 = split(//, $s1);
    my @ar2 = split(//, $s2);

    for (my $i = 1; $i <= $len1; ++$i)
    {
        for (my $j = 1; $j <= $len2; ++$j)
        {
            # Set the cost to 1 iff the ith char of $s1
            # 	equals the jth of $s2
            # 
            # Denotes a substitution cost. When the char are equal
            # 	there is no need to substitute, so the cost is 0
            #
            my $cost = ($ar1[$i-1] eq $ar2[$j-1]) ? 0 : 1;

            # Cell $mat{$i}{$j} equals the minimum of:
            #
            # - The cell immediately above plus 1
            # - The cell immediately to the left plus 1
            # - The cell diagonally above and to the left plus the cost
            #
            # We can either insert a new char, delete a char or
            # 	substitute an existing char (with an associated cost)
            #
            $mat{$i}{$j} = min([$mat{$i-1}{$j} + 1,
                                $mat{$i}{$j-1} + 1,
                                $mat{$i-1}{$j-1} + $cost]);
        }
    }

    # Finally, the Levenshtein distance equals the rightmost bottom cell of the matrix
    #
    # Note that $mat{$x}{$y} denotes the distance between the substrings : 1..$x and 1..$y
    return $mat{$len1}{$len2};
}

# ------------------------------------------------------------------------------
# function:	minimal element of a list
# ------------------------------------------------------------------------------
sub min
{
    my @list = @{$_[0]};
    my $min = $list[0];

    foreach my $i (@list){
        $min = $i if ($i < $min);
    }

    return $min;
}

# ------------------------------------------------------------------------------
# function:	compute the index of similarity on the basis of levenshtein distance
# ------------------------------------------------------------------------------
sub Similarity
{
    my ($s1, $s2) = @_;
    my ($len1, $len2) = (length $s1, length $s2);
    return 0 if ($len1 == 0 or $len2 == 0);
    return 1 - levenshtein($s1,$s2) / sqrt($len1 * $len2);
}

# ------------------------------------------------------------------------------
# function:	计算2点（传入经纬度）距离
# usage:	$distance = Distance($wd1, $jd1, $wd2, $jd2);
# ------------------------------------------------------------------------------
sub Distance
{

        my ($wd1, $jd1, $wd2, $jd2) = @_ ;
        my $PI= 3.1415926535898;
        my $R = 6.371229 * 1e6;
        #print "$R \n";
        my $x = ($jd2 - $jd1) * $PI * $R * cos( ( ($wd1 + $wd2) / 2) * $PI / 180) / 180;
        my $y = ($wd2 - $wd1) * $PI * $R / 180;
        my $d = sqrt($x * $x + $y * $y);
        return $d;

}

# ------------------------------------------------------------------------------
# usage: 	TestHash($hashref);
# function:	print the keys => values in $Hashref
# ------------------------------------------------------------------------------
sub TestHash
{
	my $HashRef = $_[ 0 ];
	foreach my $element ( keys %$HashRef ){
		print "$element => $$HashRef{$element}\n";
	}
}

# ------------------------------------------------------------------------------
# usage: 	$simp = Trad2simp($trad);
# function:	汉字繁体转为简体
# ------------------------------------------------------------------------------
sub Trad2simp
{
	my $trad = $_[0];
	return trad_to_simp( Decode2utf8($trad) );
}

# ------------------------------------------------------------------------------
# function:	decode to utf8
# usage: 	$utf8 = Decode2utf8($othercodes);
# ------------------------------------------------------------------------------

sub Decode2utf8 
{
        my $data = shift || return;
        return $data if Encode::is_utf8( $data );
        #print "[data]" , $data , "\n";
        #print "[is]",Encode::is_utf8( $data ) , "\n";
        my $decoder = guess_encoding( $data );
        if( ! ref $decoder )
	{
		#$decoder = guess_encoding( $data , qw/euc-cn/ );
                if( ! ref $decoder )
		{
			#print "[decoder]" ,$decoder,"\n";
                        return $decoder;
                }
        }
        my $utf8 = $decoder->decode($data);
        #print Encode::is_utf8( $utf8 ) , "\n";
        return $utf8;
}

# ------------------------------------------------------------------------------
# function:	将所数组引用的内容打印出来
# usage: 	TestArray($arrayref);
# input:	$ref_array
# output:	elements 
# ------------------------------------------------------------------------------
sub TestArray
{

	my $ArrayRef = $_[ 0 ];
	foreach my $element ( @$ArrayRef ){
		print "$element\n";
	}
}

# ------------------------------------------------------------------------------
# function:	数组内元素去去重
# usage: 	$UnifyArrayRef = UnifyArray($ArrayRef);
# input:	对象数组引用
# output:	结果数组引用
# ------------------------------------------------------------------------------
sub UnifyArray
{
	my $ArrayRef = $_[0];
	my %count;
	my @uniq = grep { ++$count{$_} < 2; } @$ArrayRef; 
	return \@uniq;
}

# ------------------------------------------------------------------------------
# function:	统计文件行数
# usage: 	$rows = Rows($file);
# input:	file
# return:	rows
# ------------------------------------------------------------------------------
sub Rows
{
	my $file = $_[0];
	my $lines = 0;
	open FH , "<" , $file or die "$!";
	while (sysread FH, my $buffer, 4096) {
		$lines += ($buffer =~ tr/\n//);
	}
	close FH;
	return $lines;
}

# ------------------------------------------------------------------------------
# function:	判断point是否在Polygon内
# return:	1:是，0：否
# ------------------------------------------------------------------------------
sub Contain
{
	
	my $refArray = $_[0];	# my @p = ([1,2], [2,4], [2,2], [1.5,0],[1,2]);
	my $point = $_[1];	# my $point = [1.5,2];
	my $poly = Math::Polygon -> new(@$refArray);
	
	return ($poly -> contains($point)) ? 1 : 0 ;
}

# ------------------------------------------------------------------------------
# function:	文件内信息去去重
# usage: 	UnifyFile($file);
# ------------------------------------------------------------------------------
sub UnifyFile
{
	my $file = $_[ 0 ];
	return if ! -e $file;
	system "sort $file |uniq > $file.tmp";
	system "mv $file.tmp $file";
}

# ------------------------------------------------------------------------------
# function:	standardize phoneNumber of POI (-> 86-021-38663866)
# input:	original phoneNumber , city
# return:	latest phoneNumber
# ------------------------------------------------------------------------------
sub standardizePhone
{

    my $phone = Wide2Half($_[0]);
    my $city = $_[1];
    chomp($phone);

    my $code = $areacode{$city};
    # phone字段中所有木有城市邮编的8位号码前添加城市代码
    while($phone =~ /(?<!\-)(\d{8})(?!\d)/gi)
    {
	my $d8 = $1;
	$phone =~ s/$d8/$code-$d8/i;
    }

    # 
    while($phone =~ /(\(*(86)?[-\s]*(\d{2,5})\)\s*\d{8})/gi)
    {

        my $num = $3;
        # 有的城市代码不规范，例如21替换为021
        if ($num !~ /^0/i)
        {
            $phone =~ s/(?<!0)($num)/0$num/i;
        }
        # 将左侧为')'的空白符删除
        $phone =~ s/(?<=\))\s//i;
        # 剩下的空白符替换为'-'
        $phone =~ s/\s/-/i;
        # 将左侧为数字的')'替换为'-'
        $phone =~ s/(?<=\d)\)/-/i;
        # 将右侧为数字的'('删除
        $phone =~ s/\((?=\d)//i;
    }
    # 这里将前面木有86的手机号添加86 正则考虑一些空白的情况
    while($phone =~ /\(*(86)?[-\s]*(1[3568]\d{9})(?!\d)/gi)
    {
	my $num = $2;
	$phone =~ s/(?<!86)$num/86-$num/i;
    }
    
    return $phone;
    
}

# ------------------------------------------------------------------------------
# function:	返回系统时间(2011-11-11 11:11:11)
# usage: 	$time = GetLocaltime('Y/M/D h:m:s');
# input:	time model (Y/M/D h:m:s)
# return:	time in given model 
# ------------------------------------------------------------------------------
sub GetLocaltime 
{
	my $date = localtime;			# Tue Nov 29 14:58:59 2011
        my $model = $_[0] // $date; 		# such as YYYY/MM/DD hh:mm:ss
        # YYYY : year in 4 digit
        # MM : month
        # DD : day
        # hh : hour
        # mm : minute
        # ss : second
        
        my @datefield = split / +/ , $date;
        my $weekday = $datefield[0];
        my $month = $month2num{ $datefield[1] };
        my $day = $datefield[2];
        $day =~ s/^(\d)$/0$1/;
        my @timefield = split /:/ , $datefield[3];
        my $hour = $timefield[0];
        my $minute = $timefield[1];
        my $second = $timefield[2];
        my $year4 = $datefield[4];

        #my $ret = $year4."-".$month."-".$day." ".$datefield[3];
	my $ret = $model;
        $ret =~ s/Y/$year4/g;
        $ret =~ s/M/$month/g;
        $ret =~ s/D/$day/g;
        $ret =~ s/h/$hour/g;
        $ret =~ s/m/$minute/g;
        $ret =~ s/s/$second/g;

        return $ret;
}

# ------------------------------------------------------------------------------
# function: 	字符串中的字母,数字,标点,符号全|半角互换
#			穷举法存储于哈希可避免ascii下的匹配错误
# usage: 	$half = Wide2Half($wide);
#		$wide = Half2wide($half);
# ------------------------------------------------------------------------------
sub Wide2Half
{
	my $str = $_[0];
	$str =~ s/(.*?)/$W2H{$1}?$W2H{$1}:$1/eg;
	$str =~ s/\/0\]/0/g;
	return $str;
}


sub Half2Wide
{
	my $str = $_[0];
	$str =~ s/(.*?)/$H2W{$1}?$H2W{$1}:$1/eg;
	$str =~ s/0/０/g;
	return $str;
}

sub getID
{
    my $poi = $_[0];
    my ($id) = ($poi =~ /(\d+)[;\s]/);
    return $id;
}

1;              # Do not remove this line