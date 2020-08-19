#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import cmd
import mysql.connector
import pandas as pd

class MysqlClient:
    def __init__(self):
        self.connection = mysql.connector.connect(
            host = "localhost",
            database = "SocialNetwork",
            user = "root",
            passwd = "root123"
        )
        self.cursor = self.connection.cursor()
    
    def runQuery(self, query, params = None):
        self.cursor.execute(query, params)
        return self.cursor
    

class SocialNetworkClient(cmd.Cmd):
    intro = 'this is social network. you can type ? to show all the commands.\n'
    prompt = '(please enter command:) '
    
    def __init__(self):
        super(SocialNetworkClient, self).__init__()
        self.mysql_client = MysqlClient()
        self.user_id = None

    def do_signup(self, arg):
        username = input("please enter your username: ")
        if username != "":
            user_id = self.mysql_client.runQuery("insert into User (username) values ('{}');".format(username)).lastrowid
            self.mysql_client.connection.commit();
            print("User {} has been created. the user id is {}".format(username, user_id))
        else: 
            print("username can't be None.");

    def do_login(self, arg):
        username = input("please enter your username: ")
        user_info = self.mysql_client.runQuery("select * from user where username = '{}';".format(username)).fetchall()
        self.user_id = user_info[0][0]
        if user_info:
            print("{} Login successfully! your user id is {}".format(username, self.user_id))
        else:
            print("username doesn't exist! ")

    def do_logout(self, arg):
        print("You have been logged out!")
        self.user_id = None
        
    def do_current_userInfo(self,arg):
        if(self.user_id != None):
            user_id = self.user_id;
            user_info = self.mysql_client.runQuery("select * from user where user_ID = '{}';".format(user_id)).fetchall()
            if user_info:
                print("Here is all the user information:"+ "\n" + "(UserID, username, email, phone_NO, gender,vocation,religion,birthday)"+"\n"+" {}"
                      .format(user_info))
            else:
                print("username doesn't exist! ")
        else: 
            username = input("please enter the username: ")
            user_info = self.mysql_client.runQuery("select * from user where username = '{}';".format(username)).fetchall()
            if user_info:
                print("Here is all the user information:"+ "\n" + "(username, email, phone_NO, gender,vocation,religion,birthday) {}"
                      .format(user_info))
            else:
                print("username doesn't exist! ")

                
    def do_create_post(self, arg):
        topic = input("please create your post topic: ")
        content_type = input("please identify your content type(text, image, link)")
        content = input("please create your post content: ")
        try:
            if topic and content_type and content:
                topic_id = None
                topic_id_info = self.mysql_client.runQuery("SELECT topic_ID FROM Topic WHERE topic_name = '{}';"
                                                           .format(topic)).fetchall()
                if topic_id_info:
                    topic_id = topic_id_info[0][0]
                    print("content created successfully in {}! the topic id is {}".format(topic ,topic_id))
                else:
                    # Insert the topic if topic does not exist
                    topic_id = self.mysql_client.runQuery("INSERT INTO Topic VALUES (%s, %s);", (topic_id, topic)).lastrowid
                # Insert the post with the topic
                if topic_id:
                    post_id = self.mysql_client.runQuery("INSERT INTO Post (user_ID, content_type) VALUES (%s, %s);", (self.user_id, content_type)).lastrowid
                    if(content_type == "text"):
                        self.mysql_client.runQuery("INSERT INTO text (user_ID, post_ID, txt) VALUES (%s, %s, %s);", (self.user_id, post_id, content)).lastrowid
                    elif(content_type == "image"):
                        self.mysql_client.runQuery("INSERT INTO image (user_ID, post_ID, image_location) VALUES (%s, %s, %s);", (self.user_id, post_id, content)).lastrowid
                    elif(content_type == "link"):
                        self.mysql_client.runQuery("INSERT INTO link (user_ID,post_ID, link) VALUES (%s, %s, %s);", (self.user_id, post_id, content)).lastrowid
                    else: 
                        print("please enter the correct content type.")
                    # Insert into Post_Topic 
                    print("content created successfully in the topic {}! the topic id is {}. The post id is {}".format(topic ,topic_id, post_id))
                    if post_id and (content_type == "text" or content_type == "image" or content_type == "link"):
                        self.mysql_client.runQuery("INSERT INTO Post_Topic VALUES (%s, %s);", [post_id, topic_id]).lastrowid
                        self.mysql_client.connection.commit()
                        
        except mysql.connector.Error as error :
            print("Create post failed with error: {}".format(error))
            self.mysql_client.connection.rollback()


    def do_create_group(self, arg):
        group_name = input("Input the group name: ")
        invitee_id = input("Input the invitee user ID: ")

        if not (group_name and invitee_id):
            print("Missing input. Create group failed")
            return
        try:
            group_id = self.mysql_client.runQuery("insert into _Groups (group_id, group_Name) values (NULL, '{}');".format(group_name)).lastrowid
            self.mysql_client.cursor.executemany("insert into Group_Member (group_ID, user_ID) values (%s, %s);", 
                                                 [(group_id, self.user_id),(group_id, int(invitee_id))])
            self.mysql_client.connection.commit()
            print("You and user {} has join group {} successfully. the following is the group information".format(invitee_id, group_id))
            group_Info_Query = self.mysql_client.runQuery("select * from _groups inner join group_member using(group_ID);").fetchall()
            group_Info = pd.DataFrame(group_Info_Query, columns=[
                 'group_ID',
                 'group_Name',
                 'user_ID'
            ])
            print(group_Info)
        except mysql.connector.Error as error :
            print("Create group failed with error: {}".format(error))
            self.mysql_client.connection.rollback()


    def do_follow_group(self, arg):
        group_id = input("Input the group ID: ")

        if not group_id:
            print("Missing input. Follow group failed")
            return
        try:
            result = self.mysql_client.runQuery("select * from _groups where group_ID = {}".format(group_id)).fetchall()

            if not result:
                print("Group not found!")
            else:
                # follow group if group exist
                self.mysql_client.runQuery("insert into Group_Member (group_ID, user_ID) values (%s, %s);", (group_id, self.user_id))
                self.mysql_client.connection.commit()
                print("You have follow group {} successfully.".format(group_id)+" Here is the group inforamtion for all groups")
                group_Info_Query = self.mysql_client.runQuery("select * from _groups inner join group_member using(group_ID);").fetchall()
                group_Info = pd.DataFrame(group_Info_Query, columns=[
                    'group_ID',
                    'group_Name',
                    'user_ID'
                ])
                self.mysql_client.connection.commit()
                print(group_Info)
        except mysql.connector.Error as error :
            print("Follow group failed with error: {}".format(error))
            self.mysql_client.connection.rollback()


    def do_follow_topic(self, arg):
        topic_id = input("Input topic id: ")

        if not topic_id:
            print("Missing input. Follow topic failed.")
            return
        
        try:
            find_topic_query = "select * from Topic where topic_ID = {};".format(topic_id)
            result = self.mysql_client.runQuery(find_topic_query).fetchall()

            if not result:
                print("Topic not found!")
            else:
                self.mysql_client.runQuery("insert into Topic_Follower values (%s, %s);", (topic_id, self.user_id))
                self.mysql_client.connection.commit()
                print("You have follow topic {} successfully.".format(topic_id)+"Here are all the topics and their followers:"+"\n")
                follow_results = self.mysql_client.runQuery("select * from topic_follower;").fetchall()
                topic_followers = pd.DataFrame(follow_results, columns=[
                    'topic_ID',
                    'follower_user_ID'
                ])
                print(topic_followers)
        except mysql.connector.Error as error :
            print("Follow topic failed with error: {}".format(error))
            self.mysql_client.connection.rollback()


    def do_follow_user(self, arg):
        followee_id = input("please enter the id of the user you want to follow: ")

        if not followee_id:
            print("Missing input. Follow user failed!")
            return
        try:
            user_info = self.mysql_client.runQuery("select * from User where user_ID = {};".format(followee_id)).fetchall()

            if not user_info:
                print("Followee not found")
            else:
                if(int(followee_id) != self.user_id):
                    self.mysql_client.runQuery("insert into User_Follower values (%s, %s);", (followee_id, self.user_id))
                    self.mysql_client.connection.commit()
                    print("You have follow user {} successfully. the users and their follower information are listed as follows:".format(followee_id))
                    follower_query = '''
                    select * from user_follower;'''
                    follow_result = self.mysql_client.runQuery(follower_query).fetchall()
                    df = pd.DataFrame(follow_result, columns=[
                        'user_ID',
                        'follower_ID'
                    ])
                    print(df)
                    return
                else:
                    print("you can not follow yourself")
                    return
        except mysql.connector.Error as error :
            print("Follow user failed with error: {}".format(error))
            self.mysql_client.connection.rollback()       


    def do_thumbs_up(self, arg):
        post_id = input("Input post id: ")

        if not post_id:
            print("Missing input. Thumbs up failed")

        try:
            post_info = self.mysql_client.runQuery("select * from Post where post_ID = {};".format(post_id)).fetchall()

            if not post_info:
                print("Post not found")
            else:
                self.mysql_client.runQuery("update Post set thumbs_up = thumbs_up + 1 where post_ID = {};".format(post_id))
                self.mysql_client.connection.commit()
                print("You have thumbed up post {} successfully.".format(post_id))
        except mysql.connector.Error as error :
            print("Thumbs up failed with error: {}".format(error))
            self.mysql_client.connection.rollback()  
        
    
    def do_thumbs_down(self, arg):
        post_id = input("Input post id: ")

        if not post_id:
            print("Missing input. Thumbs down failed")

        try:
            result = self.mysql_client.runQuery("select * from Post where post_ID = {};".format(post_id)).fetchall()
            if not result:
                print("Post not found")
            else:
                self.mysql_client.runQuery("update Post set thumbs_down = thumbs_down + 1 where post_ID = {};".format(post_id))
                self.mysql_client.connection.commit()
                print("You have thumbed down post {} successfully.".format(post_id))
        except mysql.connector.Error as error :
            print("Thumbs down failed with error: {}".format(error))
            self.mysql_client.connection.rollback()  


    def do_respond_to_post(self, arg):
        post_id = input("Input post id: ")
        content_type = input("Input type of content you want to respond(text, image, or link):")
        response = input("Input response: ")

        if not (post_id and response):
            print("Missing input. Respond to post failed.")
        try:
            result = self.mysql_client.runQuery("select * from Post where post_ID = {};".format(post_id)).fetchall()
            if not result:
                print("Post not found")
            else:
                respond_post_id = self.mysql_client.runQuery("insert into Post (user_ID, content_type) values (%s, %s);", (self.user_id, content_type)).lastrowid
                if(content_type == "text"):
                    self.mysql_client.runQuery("insert into text (post_ID, user_id, txt) values (%s, %s, %s);", (respond_post_id, self.user_id, response))
                elif(content_type == "image"):
                    self.mysql_client.runQuery("insert into image (post_ID, user_id, image_location) values (%s, %s, %s);", (respond_post_id, self.user_id, response))
                elif(content_type == "link"):
                    self.mysql_client.runQuery("insert into link (post_ID, user_id, link) values (%s, %s, %s);", (respond_post_id, self.user_id, response))
                else:
                    print("respond type not supported!");
                self.mysql_client.runQuery("insert into Post_Respond values (%s, %s);", (post_id, respond_post_id))
                self.mysql_client.connection.commit()
                print("You have responded to post {} successfully. the cooresponding post and respond information is listed as follows".format(post_id))
                respond_query = '''
                select * from post inner join post_respond using (post_id);'''
                result = self.mysql_client.runQuery(respond_query).fetchall()
                df = pd.DataFrame(result, columns=[
                        'post_ID',
                        'user_ID',
                        'content_type',
                        'thumbs_up',
                        'thumbs_down',
                        'create_time',
                        'respond_ID'
                ])
                print(df)
        except mysql.connector.Error as error :
            print("Respond to post failed with error: {}".format(error))
            self.mysql_client.connection.rollback()  

    def do_delete_my_posts(self, arg):
        try:
            self.mysql_client.runQuery("delete from post where user_id={};".format(self.user_id))
            print("you have delete your posts successfully!")
        except mysql.connector.Error as error :
            print("delete user posts failed with error: {}".format(error))
            self.mysql_client.connection.rollback()   

    def do_get_all_posts(self, arg):
        try:
            # Get the new post from the followed topic and followed user
            get_all_post_query = '''
                select * from(
                select * from (
                    select * from (select post.*, text.txt as 'content(txt/image_location/link)' from post 
                                   inner join text using (post_ID)
                                   union select post.*, image.image_location from post 
                                   inner join image using (post_ID)
                                   union select post.*, link.link from post 
                                   inner join link using (post_ID)) as table1
                                inner join post_topic using (post_ID)) as table2
                            inner join topic using(topic_ID)) as table3 order by post_ID;'''
            result = self.mysql_client.runQuery(get_all_post_query).fetchall()

            df = pd.DataFrame(result, columns=[
                    'topic_ID',
                    'post_ID',
                    'user_ID',
                    'content_type',
                    'thumbs_up',
                    'thumbs_down',
                    'create_time',
                    'content(txt/ image location/ link)',
                    'topic_name'
            ])
            print(df.iloc[:,[1,0,2,3,4,5,6,7,8]])
        except mysql.connector.Error as error :
            print("Get all posts failed with error: {}".format(error))
            self.mysql_client.connection.rollback()    

    def do_get_my_posts(self, arg):
        try:
            query_result = self.mysql_client.runQuery(
            ''' select * from(
                select * from (
                    select * from (select post.*, text.txt as 'content(txt/image_location/link)' from post 
                                   inner join text using (post_ID) where post.user_id ={}
                                   union select post.*, image.image_location from post 
                                   inner join image using (post_ID) where post.user_id ={}
                                   union select post.*, link.link from post 
                                   inner join link using (post_ID) where post.user_id ={}) as table1
                                inner join post_topic using (post_ID)) as table2
                            inner join topic using(topic_ID)) as table3 order by post_ID;'''
                            .format(self.user_id,self.user_id,self.user_id)).fetchall()
            if not query_result:
                print("there is nothing in your post.")
            else:
                my_posts = pd.DataFrame(query_result, columns=[
                    'topic_ID',
                    'post_ID',
                    'user_ID',
                    'content_type',
                    'thumbs_up',
                    'thumbs_down',
                    'create_time',
                    'content(txt/ image location/ link)',
                    'topic_name'
                ])
                print(my_posts.iloc[:,[1,0,2,3,4,5,6,7,8]])
        except mysql.connector.Error as error :
            print("Get my posts failed with error: {}".format(error))
            self.mysql_client.connection.rollback() 


if __name__ == '__main__':
    SocialNetworkClient().cmdloop() 


# In[ ]:





# In[ ]:




