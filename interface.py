import sys
import json
import tkinter as tk
from tkinter import *
from tkinter.font import Font
from tkinter import messagebox
from preprocessing import Executor, PlannerConfig
import argparse

connection_string = ''

e = Executor(connection_string)


class App(object):

    def __init__(connection_string):
        self.root = parent
        self.root.title("Main Frame")
        self.frm_input_text = tk.Frame(self.root)
        self.frm_input_text.pack()
        self.frm_input = tk.Frame(self.root)
        self.frm_input.pack()

        self.frm_line = tk.Frame(self.root)
        self.frm_line.pack()
        canvas = Canvas(self.frm_line, width=2000, height=20)
        canvas.create_line(0, 15, 2000, 15)
        canvas.pack()

        output_font = Font(family=None, size=15)
        # exp
        self.frm_nlp_text = tk.Frame(self.root)
        self.frm_nlp_text.pack()
        self.frm_nlp = tk.Frame(self.root)
        self.frm_nlp.pack()
        # tree
        self.frm_tree_text = tk.Frame(self.root)
        self.frm_tree_text.pack()
        self.frm_tree = tk.Frame(self.root)
        self.frm_tree.pack()
        # diff
        '''
        self.frm_diff_text = tk.Frame(self.root)
        self.frm_diff_text.pack()
        self.frm_diff = tk.Frame(self.root)
        self.frm_diff.pack()
        '''
        ##
        # btt = button
        # input text for query?
        self.frm_input_t = tk.Frame(self.frm_input)
        self.frm_input_t.pack(side=LEFT)
        self.frm_input_btt = tk.Frame(self.frm_input)
        self.frm_input_btt.pack(side=RIGHT)

        # exp input text?
        self.frm_nlp_t = tk.Frame(self.frm_nlp)
        self.frm_nlp_t.pack(side=LEFT)
        self.frm_nlp_btt = tk.Frame(self.frm_nlp)
        self.frm_nlp_btt.pack(side=RIGHT)

        # tree text?
        self.frm_tree_t = tk.Frame(self.frm_tree)
        self.frm_tree_t.pack(side=LEFT)
        self.frm_tree_btt = tk.Frame(self.frm_tree)
        self.frm_tree_btt.pack(side=RIGHT)

        # another tree text?
        self.frm_tree_t = tk.Frame(self.frm_tree)
        self.frm_tree_t.pack(side=LEFT)
        self.frm_tree_btt = tk.Frame(self.frm_tree)
        self.frm_tree_btt.pack(side=RIGHT)

        '''
        self.frm_diff_t = tk.Frame(self.frm_diff)
        self.frm_diff_t.pack(side=LEFT)
        self.frm_diff_btt = tk.Frame(self.frm_diff)
        self.frm_diff_btt.pack(side=RIGHT)
        '''

        ########################################################################################

        self.input_text1 = tk.Label(
            self.frm_input_text, text='Please Input Query', font=(None, 16), width=60)
        self.input_text1.pack(side=CENTER, pady=5)
    
        self.input = tk.Text(self.frm_input_t, relief=GROOVE,
                              width=75, height=8, borderwidth=5, font=(None, 12))
        self.input.pack(side=LEFT, padx=10)

        # second input
      #  self.input2 = tk.Text(self.frm_input_t, relief=RIDGE,
      #                        width=75, height=8, borderwidth=5, font=(None, 12))
     #   self.input2.pack(side=RIGHT, padx=10)

        # view
        self.view = tk.Button(self.frm_input_btt, text="view output",
                              width=10, height=2, command=self.retrieve_input)
        self.view.pack(pady=10)

        self.clear = tk.Button(self.frm_input_btt, text="clear input",
                               width=10, height=2, command=self.clear_input)
        self.clear.pack(pady=10)

        ####
        self.nlp_text1 = tk.Label(
            self.frm_nlp_text, text='Best Query Execution Plan:', font=(None, 16), width=60)
        self.nlp_text1.pack(side=LEFT, pady=5)

        self.nlp_text2 = tk.Label(
        self.frm_nlp_text, text='Second Best Execution Plan:', font=(None, 16), width=75)
        self.nlp_text2.pack(side=RIGHT, pady=5)

        self.nlp1 = tk.Text(self.frm_nlp_t, relief=GROOVE, width=75,
                            height=8, borderwidth=5, font=(None, 12), state='disabled')
        self.nlp1.pack(side=LEFT, padx=10)

        self.nlp2 = tk.Text(self.frm_nlp_t, relief=RIDGE, width=75,
                            height=8, borderwidth=5, font=(None, 12), state='disabled')
        self.nlp2.pack(side=RIGHT, padx=10)

        self.placeholder1 = tk.Label(self.frm_nlp_btt, width=10)
        self.placeholder1.pack()

        ####
        self.tree_text1 = tk.Label(
            self.frm_tree_text, text='Best Query Tree Structure:', font=(None, 16), width=60)
        self.tree_text1.pack(side=LEFT, pady=5)
        self.tree_text2 = tk.Label(
            self.frm_tree_text, text='Second Best Query Tree Structure: ', font=(None, 16), width=75)
        self.tree_text2.pack(side=RIGHT, pady=5)

        
        self.tree1 = tk.Text(self.frm_tree_t, relief=GROOVE, width=75,
                             height=8, borderwidth=5, font=(None, 12), state='disabled')
        self.tree1.pack(side=LEFT, padx=10)
        self.tree2 = tk.Text(self.frm_tree_t, relief=RIDGE, width=75,
                             height=8, borderwidth=5, font=(None, 12), state='disabled')
        self.tree2.pack(side=RIGHT, padx=10)

        self.placeholder2 = tk.Label(self.frm_tree_btt, width=10)
        self.placeholder2.pack()

        self.placeholder3 = tk.Label(self.frm_diff_text, width=60)
        self.placeholder3.pack(pady=5)

       # self.diff_text = tk.Label(
       #     self.frm_diff_text, text='Differences Between Two QEPs and Reasons:', font=(None, 16), width=60)
      #  self.diff_text.pack(side=LEFT, pady=5)

       # self.diff = tk.Text(self.frm_diff_t, relief=GROOVE, width=155,
       #                     height=15, borderwidth=5, font=(None, 12), state='disabled')
       # self.diff.pack(side=LEFT, padx=10)

        self.clear_out = tk.Button(
            self.frm_diff_btt, text="clear output", width=10, height=2, command=self.clear_output)
        self.clear_out.pack(pady=10)

        self.quit_ = tk.Button(self.frm_diff_btt, text="quit program",
                               width=10, height=2, command=self.quitprogram)
        self.quit_.pack(pady=10)

        self.connection_string = connection_string

    def retrieve_input(self):
        global query
        global desc
        global result
        query = self.input.get("1.0", END)

        result_best = e.get_best_plan(query)
        result_second_best = e.get_second_best_plan(query)

        result_best_obj = json.loads(json.dumps(result_best))
        result_second_best_obj = json.loads(json.dumps(result_second_best))

        result_bst_nlp = self.get_description(result_obj)

      #  result_best_tree = self.get_tree(result_obj)
      #  result_second_best_tree = self.get_tree(result_obj)

        self.nlp1.configure(state='normal')
        self.nlp2.configure(state='normal')
       # self.tree1.configure(state='normal')
       # self.tree2.configure(state='normal')

        self.nlp1.delete("1.0", END)
        self.nlp1.insert(END, result_old_nlp)
        self.nlp2.delete("1.0", END)
        self.nlp2.insert(END, result_new_nlp)

      #  self.tree1.delete("1.0", END)
      #  self.tree1.insert(END, result_old_tree)
      #  self.tree2.delete("1.0", END)
      #  self.tree2.insert(END, result_new_tree)

    def clear_input(self):
        self.input.delete("1.0", END)

    def clear_output(self):
        self.nlp1.delete("1.0", END)
        self.nlp2.delete("1.0", END)
      #  self.tree1.delete("1.0", END)
      #  self.tree2.delete("1.0", END)
        self.nlp1.configure(state='disabled')
        self.nlp2.configure(state='disabled')
      #  self.tree1.configure(state='disabled')
      #  self.tree2.configure(state='disabled')    

    def quitprogram(self):
        result = messagebox.askokcancel(
            "Quit program.", "Are you sure?", icon='warning')
        if result == True:
            self.root.destroy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--conn', help='postgresql connection string')
    
    args = parser.parse_args()
    connection_string = args.conn

    e = Executor()
    root = tk.Tk()
    app = App(connection_string)
    root.geometry('1500x1000+0+0')
    root.mainloop()