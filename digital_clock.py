import tkinter as tk
import time
import pytz

class DigitalClock:
    def __init__(self, master):
        self.master = master
        self.master.title('Digital Clock')

        # Create a label to display the time
        self.label = tk.Label(master, font=('Helvetica', 48), bg='black', fg='white')
        self.label.pack(pady=20)

        # Create a dropdown for time zones
        self.timezone_var = tk.StringVar(value='UTC')
        self.timezone_menu = tk.OptionMenu(master, self.timezone_var, *pytz.all_timezones)
        self.timezone_menu.pack(pady=20)

        self.update_clock()

    def update_clock(self):
        timezone = self.timezone_var.get()  # Get the selected timezone
        tz = pytz.timezone(timezone)
        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time() + tz.utcoffset(datetime.datetime.now()).total_seconds()))
        self.label.configure(text=current_time)
        self.master.after(1000, self.update_clock)

if __name__ == '__main__':
    root = tk.Tk()
    clock = DigitalClock(root)
    root.mainloop()
