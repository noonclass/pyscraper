# -*- coding: utf-8 -*-

import re
import os
import time

IDX = 0
SPLIT = 0

##MAIN

if __name__ == '__main__':
    src = open(ur'D:/Workspace/ScrapySpider/spider.sql', 'rb')
    dst = open(ur'D:/Workspace/ScrapySpider/dst.sql', 'wb')
    
    for line in src:
        IDX += 1
        #@ json编解码特殊处理, 替换post_excerpt中的caption字段中的字符画，'\\'替换为'\\\\'
        ## INSERT INTO `wp_posts` (`post_title`, `post_excerpt`) VALUES ('してるよ\(•ㅂ•)/', '\u3057\u3066\u308b\u3088\\(\u2022\u3142\u2022)/\n');
        if line.startswith('INSERT INTO `wp_comments`') or line.startswith('INSERT INTO `wp_commentmeta`'):
            pass
        else:
            line = re.sub(r"\\\\", r"\\\\\\\\", line)
        
        ## 替换转义字符'\'为'\\', 其前导不能为'\'; 后导不能为'\',不能为'''(单引号),不能为'"'. 即'\\'不替换，'\''和'\"'不替换
        ## INSERT INTO `wp_comments` (`comment_content`, `user_id`) VALUES ('おめでとうございます\( ˆoˆ )/新商品美味しそうです♡', 374849922);
        line = re.sub(r"([^\\])\\([^\\'\"])", r"\1\\\\\2", line) #@ \n\n  2 \\n\n
        line = re.sub(r"([^\\])\\([^\\'\"])", r"\1\\\\\2", line) #@ \\n\n 2 \\n\\n
        
        ## 替换字段未部，让MySQL正确处理
        ## INSERT INTO `wp_comments` (`comment_content`, `user_id`) VALUES ('/_____\', 182694555);
        line = re.sub(r"([^\\])\\',", r"\1\\\\',", line)
        
        ## 文件切割/10000行
        if IDX > 10000000 and line.startswith(r'INSERT'):
            dst.close()
            os.rename(ur'D:/Workspace/ScrapySpider/dst.sql', ur'D:/Workspace/ScrapySpider/dst_{:0>2}_{}.sql'.format(str(SPLIT), time.strftime("%Y%m%d_%H%M%S")))
             
            dst = open(ur'D:/Workspace/ScrapySpider/dst.sql', 'wb')
            IDX = 0
            SPLIT += 1
        
        ## ID临时改为32位系统
        #line = re.sub(r"1[45678](\d{7,8})(\d{9})", r"\2", line)
        dst.write(line)
    
    src.close()
    dst.close()
    os.rename(ur'D:/Workspace/ScrapySpider/dst.sql', ur'D:/Workspace/ScrapySpider/dst_{:0>2}_{}.sql'.format(str(SPLIT), time.strftime("%Y%m%d_%H%M%S")))