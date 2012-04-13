#!/usr/bin/perl
# ==============================================================================
# function:     POI PipeLine - clean & make_star
# create:       2011.12.25
#		繁体化简体，全角转半角，非法poi名称过滤
# modify:	2012.4.12
#		加入计算星级逻辑一并输出（反链的获取是由爬虫项目完成的）
# author:       kiwi
# 
# ==============================================================================


use 5.12.4;
no strict "refs";
use autodie;
use lib qw(/home/lifeix/桌面/PipeLine_双子星/Project);
use Mod_Pipe;
use DBI;
use utf8;
use File::Find;
use String::Similarity::Group ':all';

binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');
# ------------------------------------------------------------------------------
my $time = Mod_Pipe::GetLocaltime('YMD');

# 
my $dir_map = $Mod_Pipe::dir_map;
my $dir_clean = $Mod_Pipe::dir_clean;
my $dir_cluster = $Mod_Pipe::dir_cluster;
my $dir_bad = $Mod_Pipe::dir_bad;
my $dir_dup = $Mod_Pipe::dir_dup;

my $step = $Mod_Pipe::step ;
# ------------------------------------------------------------------------------
# 重要站点list_indexc存入数组 @index
# ------------------------------------------------------------------------------
my @index ;	
open INDEX , "<" , "./list_sites" ;
while(<INDEX>)
{
    next if /^\s*$|#/;		# 跳过空白行与注释行
    chomp;
    my ($site , undef) = split " " , $_ ;
    push @index , $site ;
}
close INDEX;


# ------------------------------------------------------------------------------
# list_cats存入映射表 %cats (catName => catId)
#		    %star_in_cat( catId => (star_min,star_max) )
# ------------------------------------------------------------------------------
my (%catName2Id,%star_in_cat);
open CATS , "<" , "./list_cats" ;
while (<CATS>)
{
    chomp;
    next if /^$|#/;	# 跳过空白行与注释行
    my ($weight,$catName,$catId,$star_range) = split("\t",Mod_Pipe::Decode2utf8($_));
    $star_range =~ s/\s//gi;
    $catName2Id{$catName} = $catId;
    ($star_in_cat{$catId}[0],$star_in_cat{$catId}[1]) = split "," , $star_range ;
}
close CATS;
#Mod_Pipe::TestHash(\%star_in_cat);

# ------------------------------------------------------------------------------
# 遍历目标文件夹的所有文件执行clean & star操作
# ------------------------------------------------------------------------------
find(\&clean, "$dir_map");

#find(\&dup, "$dir_cluster");

# ------------------------------- functions ------------------------------------
# 从./Map/中拿文件,先全转半，繁转简
# 然后非法名称的POI写入./Bad/bad文本,合法的写入./Clean/
sub clean
{
    my $file_map = Mod_Pipe::Decode2utf8($_);
    my $file_clean = $dir_clean.$file_map;
    my $file_bad = $dir_bad."bad";
    unless(-e $file_clean)
    {
        # 读取Map文件夹中的原始数据
        open MAP , "<" , $file_map or die "$file_map : $!" ;
	
	open BAD, ">>" ,$file_bad or die "$!";
	binmode(BAD, ':encoding(utf8)');
	
	open CLEAN , ">>" , $file_clean or die "$!";
	binmode(CLEAN, ':encoding(utf8)');
	
        while(<MAP>)
        {
            chomp;
            my $temp = $_ ;
	    #$temp =~ s/\\x3c\/?b\\x3e//ig; 	# 这个是爬虫没过滤干净的字符
            my $data = Mod_Pipe::Trad2simp( Mod_Pipe::Wide2Half( Mod_Pipe::Decode2utf8($temp) ) );
	    
            #my ($name,$address,$lng,$lat,$catName,$catId,$phoneNumber,$sites) = split(/\t/,$data);
	    my @info = split("\t",$data);
	    my $name = $info[0];
	    my $catId = $info[5];
	    my $sites = $info[7];
	    
	    my @sites = split(",",$sites);
	    
	    my ($num,$star,%temp) ;
	    #
	    foreach ( @sites )
	    {
		my $site;
		if ($_ =~ /baidu/){
		    $site = $_;
		}
		# 如果不是baidu的子域名，则只考虑一级域名
		else{
		    ($site) = $_ =~ /[a-z.]*?(\w+\.(com|net|info|org|gov|cn)(\.cn)*)/i;
		}
	        # 反链去重
	        $temp{$site} = 1 ;
	    }
	    
	    # 统计反链非重复重点站点个数
	    foreach (keys %temp)
	    {
		my $site = $_ ;
		if (grep { $_ eq $site } @index){
		    #print $site.",";	# 打印匹配的重要一级域名
		    $num ++;
		}
	    }
	    #say $star_in_cat{$catId}[0].",".$star_in_cat{$catId}[1];
	    # 得到反链重要站点个数后，开始计算星级

	    given($num){
		
		my ($star_min,$star_max) = ($star_in_cat{$catId}[0],$star_in_cat{$catId}[1]);
		
		when($_ > 8){
		    $star = $star_max;
		}
		when ($_ == 0){
		    $star = ($star_min + $star_max) / 2 ;
		}
		default{
		    my $star_make = $star_min + int($_ / 4);
		    $star = ($star_make > $star_max)? $star_max : $star_make ;
		}
		
	    }

            # 统计poi名称的数字个数,包括阿拉伯/简繁体数字
            my $length_d = () = $name =~ /[1-9一二三四五六七八九十壹贰叁肆伍陆柒捌玖拾]/gi;
	    
	    my $info_clean = join "\t" , map {$info[$_]} 0..6 ;
	    
	    # 对poi名称长度作限制，同时也不能全部为数字（包括数字汉字）
            if (length($name) > 32 or $length_d == length($name) ){
                print BAD $info_clean."\n";
            }
	    else{
		print CLEAN $info_clean."\t".$star."\n";
            }
	      
        }
	close MAP ;
	close BAD ;
	close CLEAN ;
    }
}

=pod  坑爹啊 有木有 木有人工部分了~下面滴白写了，还调试了那么久
sub dup
{
    my $file_cluster = Mod_Pipe::Decode2utf8($_);
    my ($batch) = $file_cluster =~ /(\d{8})/;		
    my $file_dup = $dir_dup.$batch;			# 程序判定结束的重复组ID
    my $file_dup_man = $dir_dup.$batch."_man";		# 人工判定结束的重复组ID
    my $file4man = $dir_cluster.$batch."_4man";		# 等待人工处理的j聚类组文件
    if ($file_cluster =~ /^\d{8}$/)
    {
	
	# 处理聚类组，将程序判定为重复POI的ID写入dup文件
	unless (-e $file_dup)
	{
	    my @temp;
	    open FH , "<" , $file_cluster or die "$!";
	    while(<FH>)
	    {
	        chomp;
	        # 依次遍历抛出来的聚类组
	        if($_ !~ /^\s*$/){
	            push(@temp,Mod_Pipe::Decode2utf8($_));
	            next;
	        }
	        # 将程序判定为重复的POI组的ID写入dup文件
	        else
	        {
		    my $len = scalar(@temp);
		    my @dup = grep {/Y/} @temp;
		    my $len_y = scalar(@dup);
		    if (scalar(@dup) > 1)
		    {
			my @dup_id = map {Mod_Pipe::getID($_)} @dup ;
			open DUP , ">>" , $file_dup or die "$!";
			print DUP join("," , @dup_id)."\n";
			close DUP;
		    }
		    else
		    {
			foreach(@temp){
			    chomp;
			    open MAN , ">>" , $file4man or die "$!";
			    print MAN  $_."\n";
			    close MAN;
			}
			open MAN , ">>" , $file4man or die "$!";
			print MAN "\n";
			close MAN;
		    }
		    @temp = ();
		}
	    } #遍历文件结束
	}
    }
    Mod_Pipe::UnifyFile($file_dup);
    # 处理人工判定结束的文件
    if($file_cluster =~ /_d/)
    {
	unless (-e $file_dup_man)
	{
	    my @temp;
	    open FHD , "<" , $file_cluster or die "$!";
	    while(<FHD>)
	    {
	        chomp;
	        # 依次遍历抛出来的聚类组
	        if($_ !~ /^\s+$/)
		{
	            push(@temp,Mod_Pipe::Decode2utf8($_));
	            next;
	        }
	        # 将人工判定为重复的POI组的ID写入dup文件
	        else
	        {
		    my @dup = grep {/y/i} @temp;
		    if(scalar(@dup) > 1){
			my @dup_id = sort{ $a <=> $b } map {Mod_Pipe::getID($_);} @dup;
                        #say join "," , @dup_id;
			open DUPMAN , ">>" , $file_dup_man or die "$!";
			print DUPMAN join("," , @dup_id)."\n";
			close DUPMAN;
                    }
		    @temp = ();
		}
	    } #遍历文件结束
	}
    }
    Mod_Pipe::UnifyFile($file_dup_man);
}





=pod	# 策略有所调整 以下代码先放着 等要处理原始库的时候再用
sub cluster
{
    my $file = Mod_Pipe::Decode2utf8($_);			# 等待聚类的文件
    my $file_cluster = $dir_cluster."C/".$file;			# 生成好聚类组的文件
    my $file_cluster_m = $dir_cluster."C/".$file."_4man";	# 准备提交给人工的文件
    my $file_cluster_d = $dir_cluster."D/".$file."_d";		# 人工判定结束的文件
    my $file_dup = $Mod_Pipe::dir."dup";			# 记录重复组ID的log文件
    my $dir_back = $Mod_Pipe::dir_backup;			# 备份文件夹路径
    
    # 判断./Cluster/下有木有文件，避免空跑(因为要扫全国，一跑就是一天)，开始抛聚类组
    if (-f $file)
    {
	# 判断该文件处理过木有
	unless(-e $file_cluster)
	{
	    open LL , "<" , $Mod_Pipe::file_centerll or die "$!";
	    # 遍历聚类组中心点
	    while(<LL>)
	    {
		chomp;
		print "Now the ll is $_ .\n";
	        my ($lng_ll,$lat_ll) = split(",",$_);
	        my (@data,%data,%name2id);
	        open FH , "<" , $file or die "$!";
	        while(<FH>){
	            chomp ;
	            my $data = Mod_Pipe::Decode2utf8($_);
	            my ($id,$name,$address,$lng,$lat,$category,$phoneNumber) = ($data =~ /(\d+);(.*?);(.*?);(.*?);(.*?);(.*?);(.*?)/);
	            #say "$id,$name";
	            # 如果点在参照中心点周边500米正方形内
	            if($lat > $lat_ll - $step && $lat < $lat_ll + $step && $lng > $lng_ll - $step && $lng < $lng_ll + $step)
	            {
	                    $data{$id} = $data ;
	                    $name =~ s/"//g;
	                    push(@data,$name);
	                    $name2id{$name} .= " $id";
	            }
	        }
	        my @groups = groups_hard( $Mod_Pipe::index_similarity, \@data );
	        foreach(@groups)
	        {
	                if(scalar(@$_) > 1){
	                    my @temp;
	                    foreach my $element ( @$_ ){
	            		push( @temp,split(" ",$name2id{$element}) );
	                    }
	                    foreach my $id ( @{Mod_Pipe::UnifyArray(\@temp)} ){
		                    open CLUSTER , ">>" ,$file_cluster or die "$file_cluster:$!";
		                    print CLUSTER $data{$id}."\n";
			    }
			    # 组与组之间用空行隔开
			    open CLUSTER , ">>" ,$file_cluster ;
			    print CLUSTER "\n";
			}
		}
	    }
	}
	# 抛聚类组处理完之后，删除文件，留着也碍眼
	unlink $file;
	print "抛聚类组结束！\n";
    }
    
    
    # 处理抛出来的聚类组，将程序能判定为重的数据记录log，剩下的交给人工 ----------------------------这个狠不好写啊！！擦！！
    if (-f $file_cluster)
    {
	open FC , "<" , $file_cluster or die "$!";
	# 数组temp存储聚类组
	# 数组dup存储程序判定为重的聚类组
	# 数组man存储提交给人工处理的聚类组
	my (@temp,@dup,@man);
	while(<FC>)
	{
	    chomp;
	    # 依次遍历聚类组
	    if($_ !~ /^\s*$/){
	        push(@temp,Mod_Pipe::Decode2utf8($_));
	        next;
	    }
	    # 开始判定
	    else
	    {
		my $target = $temp[0];
		my ($id,$lng,$lat,$addr,$phone,$name,$cat) = ($target =~ /(\d+);(.*?);(.*?);(.*?);(.*?);(.*?);(.*?)/);
		#shift(@temp);
		push @dup , $target;
		foreach( splice(@temp,1) )
		{
		    my $data = $_;
		    my ($id_d,$lng_d,$lat_d,$addr_d,$phone_d,$name_d,$cat_d) = ($data =~ /(\d+);(.*?);(.*?);(.*?);(.*?);(.*?);(.*?)/);
		    if(Mod_Pipe::Similarity($addr_d,$addr) > 0.6 && Mod_Pipe::Similarity($name_d,$name) > 0.707 or $name_d eq $name){
			if ($name_d =~ /atm|门/i){
			    push @dup , $data;
			}
			else{
			    unshift @dup , $data;
			}
		    }
		}
		# 如果程序能判定出聚类组中的重复组,将剩下的聚类组提交给人工
		if (scalar(@dup) > 1 )
		{
		    # 下面这一句是俺12月以来写的最满意的一句代码,简洁暴力,嗯嗯
		    my $dup = join " " , map { /^(\d+);/ } @dup ;
		    open DUP , ">>" , $file_dup or die "$!";
		    print DUP $dup."\n";
		    @man = grep {not $_ ~~ @dup} @temp;
		    foreach(@man){
			chomp;
			open MAN , ">>" , $file_cluster_m or die "$!";
			print MAN $_."\n";
		    }
		}
		# 如果判定条件不足，则此聚类组全部交给人工
		else{
		    foreach(@temp){
			chomp;
			open MAN , ">>" , $file_cluster_m or die "$!";
			print MAN $_."\n";
		    }
		}
		# 组之间的\n依然不能少
	        open MAN , ">>" , $file_cluster_m ;
		print MAN $_."\n";                            
	        # 清空聚类组判定容器，准备塞下一个聚类组
	        @temp = ();
		@dup = ();
		@man = ();
	    }
	}
	print "程序判重结束！\n";
    }
    
    
    # 处理人工标识完的数据，记录log，转移文件
    if (-f $file_cluster_d)
    {
	my (@temp_d,@dup_d);
	open FD , "<" , $file_cluster_d or die "$!";
	while(<FD>)
	{
	    chomp;
	    # 依次遍历人工判定的聚类组
	    if($_ !~ /^\s*$/){
	        push(@temp_d,Mod_Pipe::Decode2utf8($_));
	        next;
	    }
	    # 开始判定
	    else
	    {
		# 先找人工判定为重保留的POI
		push @dup_d  , map { /^(\d+);.*?Y/i } @temp_d;
		# 再找人工判定为重剔除的POI
		push @dup_d  , map { /^(\d+);.*?D/i } @temp_d;
		# 写入重复组数据log
		my $dup = join " " , @dup_d ;
		open DUP , ">>" , $file_dup or die "$!";
		print DUP $dup."\n";
		
		# 清空聚类组判定容器，准备塞下一个聚类组
		@temp_d = ();
		@dup_d = ();
	    }
	}
	system "mv $file_cluster_d $dir_back";
	print "人工判重结束！\n";
    }
    
}
