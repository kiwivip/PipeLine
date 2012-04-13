#!/usr/bin/perl
# ==============================================================================
# function:     POI PipeLine - cluster 
#		因为管道的人工辅助判定部分已经默认不再，暂时调整为这样的模式
#		(所有程序抛出的相似聚类组均视之为重复组对待)
# create:       2012.1.9
# author:       kiwi
# ==============================================================================

use 5.12.4;
no strict "refs";
use lib qw(/home/lifeix/桌面/PipeLine_双子星/Project);
use Mod_Pipe;
use DBI;
use utf8;
#use File::Find;


use String::Similarity::Group ':all';

binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');
# ------------------------------------------------------------------------------
# input:描述批次的参数 如20120102 - 目前使用townfile_local表中的tag字段
my $flag = shift;
# ------------------------------------------------------------------------------

my $file_dup = $Mod_Pipe::dir_dup.$flag."_dup";
my $step = 0.005;

# 如果批次已存在，程序会抛出警告并终止运行
die "Batch : $flag"."_dup already exist !" unless (!(-e $file_dup)) ;

# ------------------------------------------------------------------------------
# 扫描数据库中新增的POI（测试表）
# ------------------------------------------------------------------------------
my $database = "d01_backup" ;
my $host = $Mod_Pipe::host ;
my $user = $Mod_Pipe::username ;
my $password = $Mod_Pipe::password ;
my $dsn = "DBI:mysql:database=$database;host=$host" ;
my $dbh = DBI -> connect($dsn, $user, $password, {'RaiseError' => 1} ) ;
$dbh-> do ("SET NAMES UTF8");
# 只查询未被隐藏的POI数据
my $sth = $dbh -> prepare(
    #"SELECT * FROM townfile_local where batchNo=$flag and status=0 ;"
    qq{SELECT t.localId,l.lng,l.lat FROM townfile_local t left join tape_local l on t.localId = l.id where t.tag = $flag;}
);
$sth -> execute();
# ------------------------------------------------------------------------------
my %ids;					# 存储已存在于相似聚类组的ID

#say "Batch:$flag has ".$sth->rows."POIs ❤ ";	# 打印批次数据的总数量

# 遍历新增的POI 提出附近的原有数据 生成聚类组
while (my $ref = $sth -> fetchrow_hashref())
{
    
    my (@data,%data,%name2id);
    my $id_b = $ref -> {'localId'} ;
    #my $name = $ref -> {'name'};
    #my $address = $ref -> {'fullAddress'};
    my $lat_b = $ref -> {'lat'};
    my $lng_b = $ref -> {'lng'};
    
    print "Now the ID = $id_b ❤ \n";		# 控制台打印进度
    
    # 如果扫描到的新数据的ID已存在聚类组中，遍历下一个
    unless (exists $ids{$id_b})
    {
	# 把新数据周边step米的所有数据提出来吧
	my $sth2 = $dbh -> prepare(
	    qq{SELECT t.localId,t.localName,l.lng,l.lat FROM townfile_local t left join tape_local l on t.localId = l.id where
	    l.lng>$lng_b-$step and l.lng<$lng_b+$step and l.lat>$lat_b-$step and l.lat<$lat_b+$step ;}
	);
	$sth2 -> execute();
	
	# 遍历正方形中的所有POI
	while (my $ref2 = $sth2 -> fetchrow_hashref())
	{
	    my $id = $ref2 -> {'localId'} ;
	    my $name = $ref2 -> {'localName'};
	    #my $address = $ref2 -> {'fullAddress'};
	    my $lat = $ref2 -> {'lat'};
	    my $lng = $ref2 -> {'lng'};
	    
	    my $poi = Mod_Pipe::Decode2utf8("$id;$name;$lng;$lat");
	    $data{$id} = $poi ;
	    
	    # 如果正方形中的某POI已存在于之前抛出的相似聚类组中，则不会参与此次聚类
	    unless(exists $ids{$id})
	    {
		$name =~ s/\s+$//i;
		$name =~ s/[()（）]//gi;
		#$name =~ s/([(（]).*?([)）])//gi;	
		push @data , $name ;
		$name2id{$name} .= " $id";
	    }
	    
	}
	# 按POI名称抛聚类组,相似度index_similarity为约束系数
	my @groups = groups_hard( 0.85 , \@data );
	foreach(@groups)
	{
	    # 只考虑非孤立组
	    if(scalar(@$_) > 1)
	    {
	        my (@temp,@pois);
		    
		map {push( @temp,split(" ",$name2id{$_}) )} @$_;
		    
		# 只处理包含新进批次数据的聚类组，如果要处理所有聚类组，注释掉if就ok了
		if (join(" ", @temp) ~~ /\d{8}/)
		{
		    @pois = map {$data{$_}} @{Mod_Pipe::UnifyArray(\@temp)};
		    
		    map {$ids{Mod_Pipe::getID($_)} = 1} @pois;
		    
		    # 打印重复poi组
		    map {say $_} @pois;
		    
		    # 重复id按大小顺序一组（保留老数据，舍弃新数据）写入文本
		    my $dup_ids = join("," , sort{ $a <=> $b } map {Mod_Pipe::getID($_);} @pois);
		    open DUP , ">>" , $file_dup or die "$!";
		    print DUP $dup_ids."\n" ;
		    
		}
		print "\n";
	    }
	} #遍历聚类组结束
    }

} # 遍历新增的POI结束


$sth -> finish();
$dbh -> disconnect();




